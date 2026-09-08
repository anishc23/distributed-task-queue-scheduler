// Package broker owns every interaction with Redis: stream and consumer group
// lifecycle, the persistent pending-task index, and the atomic Lua scripts that
// make the hand-offs between components crash-safe.
//
// Deployment assumption: a single (non-clustered) Redis 7+ instance. The
// scripts declare all their keys, but the design has not been validated against
// Redis Cluster key-slot constraints.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/config"
	"github.com/anishc23/distributed-task-queue/internal/domain"
	"github.com/anishc23/distributed-task-queue/internal/scheduler/policy"
	"github.com/redis/go-redis/v9"
)

// Broker is the Redis-backed queue substrate.
type Broker struct {
	rdb    *redis.Client
	keys   Keys
	maxLen int64
	owned  bool
}

// New dials Redis using the supplied configuration.
func New(cfg *config.Config) (*Broker, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		DialTimeout:  cfg.Redis.DialTimeout.D(),
		ReadTimeout:  cfg.Redis.ReadTimeout.D(),
		WriteTimeout: cfg.Redis.WriteTimeout.D(),
		PoolSize:     cfg.Redis.PoolSize,
	})
	return &Broker{rdb: rdb, keys: NewKeys(cfg.Streams), maxLen: cfg.Streams.MaxLen, owned: true}, nil
}

// NewWithClient wraps an existing Redis client. The caller keeps ownership of
// the client and Close does not close it. Used by the benchmark harness, which
// runs many isolated namespaces over one connection pool.
func NewWithClient(rdb *redis.Client, streams config.Streams) *Broker {
	return &Broker{rdb: rdb, keys: NewKeys(streams), maxLen: streams.MaxLen}
}

// Redis exposes the underlying client for diagnostics and tests.
func (b *Broker) Redis() *redis.Client { return b.rdb }

// Keys returns the resolved key set.
func (b *Broker) Keys() Keys { return b.keys }

// Close releases the connection pool if this broker owns it.
func (b *Broker) Close() error {
	if !b.owned {
		return nil
	}
	return b.rdb.Close()
}

// Ping verifies connectivity and returns an actionable error if Redis is down.
func (b *Broker) Ping(ctx context.Context) error {
	if err := b.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis not reachable at %s: %w (start it with `make redis-up`)", b.rdb.Options().Addr, err)
	}
	return nil
}

// WaitReady blocks until Redis answers a ping or the timeout expires, retrying
// with a capped backoff.
//
// Without this, a process started before its broker exits immediately and the
// orchestrator restarts it, producing a crash loop and a misleading restart
// count during an otherwise normal rollout. Waiting is the honest behaviour:
// the dependency is not broken, it is just not up yet.
func (b *Broker) WaitReady(ctx context.Context, timeout time.Duration, log *slog.Logger) error {
	if timeout <= 0 {
		return b.Ping(ctx)
	}
	deadline := time.Now().Add(timeout)
	backoff := 100 * time.Millisecond
	const maxBackoff = 3 * time.Second

	var lastErr error
	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := b.rdb.Ping(attemptCtx).Err()
		cancel()
		if err == nil {
			if attempt > 1 && log != nil {
				log.Info("redis became reachable", "addr", b.rdb.Options().Addr, "attempts", attempt)
			}
			return nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return fmt.Errorf("interrupted while waiting for redis at %s: %w", b.rdb.Options().Addr, ctx.Err())
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("redis not reachable at %s after waiting %s: %w (start it with `make redis-up`)",
				b.rdb.Options().Addr, timeout, lastErr)
		}
		if log != nil && attempt == 1 {
			log.Warn("redis is not reachable yet, waiting",
				"addr", b.rdb.Options().Addr, "timeout", timeout.String(), "error", err)
		}

		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("interrupted while waiting for redis at %s: %w", b.rdb.Options().Addr, ctx.Err())
		case <-t.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// EnsureStreams creates the ingress and exec streams and their consumer groups
// if they do not exist. It is safe to call concurrently from every process.
func (b *Broker) EnsureStreams(ctx context.Context) error {
	type group struct{ stream, name string }
	for _, g := range []group{
		{b.keys.Ingress, b.keys.SchedulerGroup},
		{b.keys.Exec, b.keys.WorkerGroup},
	} {
		// MKSTREAM creates the stream if absent; "$" would skip entries that
		// already exist, so groups start at 0 to consume any backlog.
		err := b.rdb.XGroupCreateMkStream(ctx, g.stream, g.name, "0").Err()
		if err != nil && !isBusyGroup(err) {
			return fmt.Errorf("create consumer group %s on %s: %w", g.name, g.stream, err)
		}
	}
	return nil
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Reset deletes every key in the namespace. Used between benchmark runs so that
// results from different runs can never be mixed.
func (b *Broker) Reset(ctx context.Context) error {
	if err := b.rdb.Del(ctx, b.keys.All()...).Err(); err != nil {
		return fmt.Errorf("reset namespace %s: %w", b.keys.Namespace, err)
	}
	return nil
}

// Submit appends a task to the ingress stream. Producers call this and nothing
// else; they are unaware of which scheduling policy is running.
func (b *Broker) Submit(ctx context.Context, t domain.Task) error {
	payload, err := t.MarshalJSONString()
	if err != nil {
		return err
	}
	args := &redis.XAddArgs{
		Stream: b.keys.Ingress,
		Values: map[string]any{"task": payload},
	}
	if b.maxLen > 0 {
		args.MaxLen = b.maxLen
		args.Approx = true
	}
	if err := b.rdb.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("submit task %s: %w", t.ID, err)
	}
	return nil
}

// SubmitBatch pipelines many submissions. Ordering inside the ingress stream is
// preserved because a pipeline is sent on a single connection.
func (b *Broker) SubmitBatch(ctx context.Context, tasks []domain.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	pipe := b.rdb.Pipeline()
	for _, t := range tasks {
		payload, err := t.MarshalJSONString()
		if err != nil {
			return err
		}
		args := &redis.XAddArgs{
			Stream: b.keys.Ingress,
			Values: map[string]any{"task": payload},
		}
		if b.maxLen > 0 {
			args.MaxLen = b.maxLen
			args.Approx = true
		}
		pipe.XAdd(ctx, args)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("submit batch of %d tasks: %w", len(tasks), err)
	}
	return nil
}

// IngressMessage is one entry read from the ingress stream.
type IngressMessage struct {
	ID   string
	Task domain.Task
}

// ReadIngress reads up to count new ingress entries for the given consumer,
// blocking for at most block. It returns an empty slice when nothing arrived.
func (b *Broker) ReadIngress(ctx context.Context, consumer string, count int64, block time.Duration) ([]IngressMessage, error) {
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    b.keys.SchedulerGroup,
		Consumer: consumer,
		Streams:  []string{b.keys.Ingress, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ingress: %w", err)
	}
	var out []IngressMessage
	for _, stream := range res {
		for _, msg := range stream.Messages {
			raw, _ := msg.Values["task"].(string)
			task, err := domain.UnmarshalTask(raw)
			if err != nil {
				// A malformed entry must not wedge the scheduler; acknowledge
				// it so the group makes progress and report it upward.
				_ = b.AckIngress(ctx, msg.ID)
				return out, fmt.Errorf("ingress entry %s: %w", msg.ID, err)
			}
			out = append(out, IngressMessage{ID: msg.ID, Task: task})
		}
	}
	return out, nil
}

// AckIngress acknowledges ingress entries.
func (b *Broker) AckIngress(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := b.rdb.XAck(ctx, b.keys.Ingress, b.keys.SchedulerGroup, ids...).Err(); err != nil {
		return fmt.Errorf("ack ingress: %w", err)
	}
	return nil
}

// Admit inserts a ranked task into the persistent pending set. It reports
// whether the task was newly admitted; false means it was a duplicate ingress
// delivery for a task already pending, in flight or completed.
func (b *Broker) Admit(ctx context.Context, t domain.Task, key policy.Key) (bool, error) {
	payload, err := t.MarshalJSONString()
	if err != nil {
		return false, err
	}
	res, err := admitScript.Run(ctx, b.rdb,
		[]string{b.keys.Payloads, b.keys.Pending, b.keys.PendingAge, b.keys.Done, b.keys.Stats},
		t.ID, key.Member, strconv.FormatFloat(key.Score, 'g', 17, 64),
		strconv.FormatInt(t.SubmittedAtMillis(), 10), payload,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("admit task %s: %w", t.ID, err)
	}
	return res == 1, nil
}

// Dispatched describes one task handed to the worker stream.
type Dispatched struct {
	Key  policy.Key
	Task domain.Task
}

// Dispatch atomically selects up to max pending tasks in policy order and
// publishes them on the worker execution stream. maxInFlight bounds tasks that
// are dispatched but not yet terminal; zero means unlimited.
func (b *Broker) Dispatch(ctx context.Context, max int, maxInFlight int, now time.Time) ([]Dispatched, error) {
	raw, err := dispatchScript.Run(ctx, b.rdb,
		[]string{b.keys.Pending, b.keys.PendingAge, b.keys.Payloads, b.keys.Exec, b.keys.Stats, b.keys.InFlight},
		max, maxInFlight, now.UnixMilli(), b.maxLen,
	).Slice()
	if err != nil {
		return nil, fmt.Errorf("dispatch: %w", err)
	}
	if len(raw)%3 != 0 {
		return nil, fmt.Errorf("dispatch: malformed reply of length %d", len(raw))
	}
	out := make([]Dispatched, 0, len(raw)/3)
	for i := 0; i < len(raw); i += 3 {
		member, _ := raw[i].(string)
		scoreStr := fmt.Sprint(raw[i+1])
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			return nil, fmt.Errorf("dispatch: bad score %q for %s: %w", scoreStr, member, err)
		}
		payload, _ := raw[i+2].(string)
		task, err := domain.UnmarshalTask(payload)
		if err != nil {
			return nil, fmt.Errorf("dispatch: %w", err)
		}
		out = append(out, Dispatched{Key: policy.Key{Score: score, Member: member}, Task: task})
	}
	return out, nil
}

// ExecMessage is one task delivered to a worker.
type ExecMessage struct {
	ID           string
	Task         domain.Task
	DispatchedAt time.Time
}

// ReadExec reads new execution entries for a worker consumer.
func (b *Broker) ReadExec(ctx context.Context, consumer string, count int64, block time.Duration) ([]ExecMessage, error) {
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    b.keys.WorkerGroup,
		Consumer: consumer,
		Streams:  []string{b.keys.Exec, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read exec stream: %w", err)
	}
	return parseExecMessages(res)
}

func parseExecMessages(streams []redis.XStream) ([]ExecMessage, error) {
	var out []ExecMessage
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			m, err := parseExecMessage(msg)
			if err != nil {
				return out, err
			}
			out = append(out, m)
		}
	}
	return out, nil
}

func parseExecMessage(msg redis.XMessage) (ExecMessage, error) {
	raw, _ := msg.Values["task"].(string)
	task, err := domain.UnmarshalTask(raw)
	if err != nil {
		return ExecMessage{}, fmt.Errorf("exec entry %s: %w", msg.ID, err)
	}
	m := ExecMessage{ID: msg.ID, Task: task}
	if s, ok := msg.Values["dispatched_at_ms"].(string); ok {
		if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
			m.DispatchedAt = time.UnixMilli(ms)
		}
	}
	return m, nil
}

// Complete records a terminal successful completion and acknowledges the
// delivery in one atomic step. It returns false when the completion was a
// duplicate, which is expected under at-least-once delivery.
func (b *Broker) Complete(ctx context.Context, res domain.Result, msgID string) (bool, error) {
	payload, err := res.MarshalJSONString()
	if err != nil {
		return false, err
	}
	missed := 0
	if res.MissedDeadline() {
		missed = 1
	}
	service := res.ActualExecMillis
	if service <= 0 {
		service = res.ExecMillis
	}
	recorded, err := completeScript.Run(ctx, b.rdb,
		[]string{b.keys.Done, b.keys.Payloads, b.keys.Results, b.keys.Stats,
			b.keys.TenantService, b.keys.TenantCount, b.keys.InFlight, b.keys.Exec},
		res.TaskID, payload, res.TenantID, service, missed,
		time.Now().UnixMilli(), b.keys.WorkerGroup, msgID, b.maxLen,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("complete task %s: %w", res.TaskID, err)
	}
	return recorded == 1, nil
}

// RetryOutcome describes what RetryOrDeadLetter did.
type RetryOutcome int

const (
	// RetryAlreadyTerminal means the task had already completed or been
	// dead-lettered, so nothing was redelivered.
	RetryAlreadyTerminal RetryOutcome = 0
	// RetryRedelivered means a new attempt was published.
	RetryRedelivered RetryOutcome = 1
	// RetryDeadLettered means the retry budget was exhausted.
	RetryDeadLettered RetryOutcome = 2
)

// RetryOrDeadLetter redelivers a task with an incremented retry count, or moves
// it to the dead-letter stream when the retry budget is exhausted. The old
// delivery is acknowledged atomically with the new one, so a crash can neither
// duplicate the retry nor drop the task.
//
// budget is the maximum number of retries permitted for this task. reason is
// recorded in the dead-letter entry. msgID is the exec-stream entry being
// replaced and may be empty when there is nothing to acknowledge.
func (b *Broker) RetryOrDeadLetter(ctx context.Context, task domain.Task, budget int, reason string, msgID string) (RetryOutcome, domain.Task, error) {
	next := task
	next.RetryCount = task.RetryCount + 1
	next.MaxRetries = budget
	next.AttemptID = fmt.Sprintf("%s#%d", task.ID, next.RetryCount)

	isDead := 0
	if next.RetryCount > budget {
		isDead = 1
	}

	retryJSON, err := next.MarshalJSONString()
	if err != nil {
		return RetryAlreadyTerminal, task, err
	}
	dead := domain.DeadLetter{
		Task:        task,
		Reason:      reason,
		Attempts:    task.RetryCount + 1,
		LastAttempt: task.AttemptID,
		FailedAt:    time.Now().UTC(),
	}
	deadJSON, err := dead.MarshalJSONString()
	if err != nil {
		return RetryAlreadyTerminal, task, err
	}

	res, err := retryScript.Run(ctx, b.rdb,
		[]string{b.keys.Done, b.keys.Payloads, b.keys.Exec, b.keys.DeadLetter, b.keys.Stats, b.keys.InFlight},
		task.ID, retryJSON, deadJSON, isDead, time.Now().UnixMilli(),
		b.keys.WorkerGroup, msgID, b.maxLen,
	).Int64()
	if err != nil {
		return RetryAlreadyTerminal, task, fmt.Errorf("retry task %s: %w", task.ID, err)
	}
	return RetryOutcome(res), next, nil
}

// AutoClaim reclaims exec-stream entries that have been pending in the worker
// group for longer than minIdle, which is the visibility timeout. It returns
// the reclaimed messages and the cursor to pass on the next call.
func (b *Broker) AutoClaim(ctx context.Context, consumer string, minIdle time.Duration, start string, count int64) ([]ExecMessage, string, error) {
	msgs, next, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   b.keys.Exec,
		Group:    b.keys.WorkerGroup,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, "0-0", nil
	}
	if err != nil {
		return nil, start, fmt.Errorf("autoclaim exec stream: %w", err)
	}
	out := make([]ExecMessage, 0, len(msgs))
	for _, m := range msgs {
		parsed, err := parseExecMessage(m)
		if err != nil {
			// Entry is unusable: acknowledge it so it stops being reclaimed
			// forever, and surface the error.
			_ = b.rdb.XAck(ctx, b.keys.Exec, b.keys.WorkerGroup, m.ID).Err()
			continue
		}
		out = append(out, parsed)
	}
	return out, next, nil
}

// PendingCount returns the number of tasks waiting in the scheduler.
func (b *Broker) PendingCount(ctx context.Context) (int64, error) {
	n, err := b.rdb.ZCard(ctx, b.keys.Pending).Result()
	if err != nil {
		return 0, fmt.Errorf("pending count: %w", err)
	}
	return n, nil
}

// OldestPendingSubmit returns the submission time of the longest-waiting
// pending task. ok is false when nothing is pending.
func (b *Broker) OldestPendingSubmit(ctx context.Context) (t time.Time, ok bool, err error) {
	res, err := b.rdb.ZRangeWithScores(ctx, b.keys.PendingAge, 0, 0).Result()
	if err != nil {
		return time.Time{}, false, fmt.Errorf("oldest pending: %w", err)
	}
	if len(res) == 0 {
		return time.Time{}, false, nil
	}
	return time.UnixMilli(int64(res[0].Score)), true, nil
}

// InFlight returns the number of tasks dispatched but not yet terminal.
func (b *Broker) InFlight(ctx context.Context) (int64, error) {
	v, err := b.rdb.Get(ctx, b.keys.InFlight).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("in-flight count: %w", err)
	}
	return v, nil
}

// Stats returns the counter hash.
func (b *Broker) Stats(ctx context.Context) (map[string]int64, error) {
	raw, err := b.rdb.HGetAll(ctx, b.keys.Stats).Result()
	if err != nil {
		return nil, fmt.Errorf("stats: %w", err)
	}
	out := make(map[string]int64, len(raw))
	for k, v := range raw {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("stats field %s: %w", k, err)
		}
		out[k] = n
	}
	return out, nil
}

// TenantService returns completed service milliseconds and completed task count
// per tenant. This is the x vector used for Jain's fairness index.
func (b *Broker) TenantService(ctx context.Context) (service map[string]int64, count map[string]int64, err error) {
	svcRaw, err := b.rdb.HGetAll(ctx, b.keys.TenantService).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("tenant service: %w", err)
	}
	cntRaw, err := b.rdb.HGetAll(ctx, b.keys.TenantCount).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("tenant count: %w", err)
	}
	parse := func(m map[string]string) (map[string]int64, error) {
		out := make(map[string]int64, len(m))
		for k, v := range m {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("tenant %s: %w", k, err)
			}
			out[k] = n
		}
		return out, nil
	}
	service, err = parse(svcRaw)
	if err != nil {
		return nil, nil, err
	}
	count, err = parse(cntRaw)
	if err != nil {
		return nil, nil, err
	}
	return service, count, nil
}

// SaveSchedulerState persists policy state so a scheduler restart resumes with
// the same virtual clocks instead of silently re-granting service.
func (b *Broker) SaveSchedulerState(ctx context.Context, state map[string]string) error {
	if len(state) == 0 {
		return nil
	}
	values := make(map[string]any, len(state))
	for k, v := range state {
		values[k] = v
	}
	if err := b.rdb.HSet(ctx, b.keys.SchedState, values).Err(); err != nil {
		return fmt.Errorf("save scheduler state: %w", err)
	}
	return nil
}

// LoadSchedulerState reads previously persisted policy state.
func (b *Broker) LoadSchedulerState(ctx context.Context) (map[string]string, error) {
	state, err := b.rdb.HGetAll(ctx, b.keys.SchedState).Result()
	if err != nil {
		return nil, fmt.Errorf("load scheduler state: %w", err)
	}
	return state, nil
}

// ReadResults reads up to count result records starting after the given stream
// ID. Pass "0" to start from the beginning. It returns the results and the ID
// to continue from.
func (b *Broker) ReadResults(ctx context.Context, start string, count int64) ([]domain.Result, string, error) {
	msgs, err := b.rdb.XRangeN(ctx, b.keys.Results, exclusive(start), "+", count).Result()
	if err != nil {
		return nil, start, fmt.Errorf("read results: %w", err)
	}
	out := make([]domain.Result, 0, len(msgs))
	cursor := start
	for _, m := range msgs {
		raw, _ := m.Values["result"].(string)
		r, err := domain.UnmarshalResult(raw)
		if err != nil {
			return out, cursor, fmt.Errorf("result entry %s: %w", m.ID, err)
		}
		out = append(out, r)
		cursor = m.ID
	}
	return out, cursor, nil
}

// exclusive turns a stream ID into the exclusive range start Redis expects.
func exclusive(id string) string {
	if id == "" || id == "0" || id == "0-0" {
		return "-"
	}
	return "(" + id
}

// ReadDeadLetters returns dead-letter records, newest last.
func (b *Broker) ReadDeadLetters(ctx context.Context, count int64) ([]domain.DeadLetter, error) {
	msgs, err := b.rdb.XRangeN(ctx, b.keys.DeadLetter, "-", "+", count).Result()
	if err != nil {
		return nil, fmt.Errorf("read dead letters: %w", err)
	}
	out := make([]domain.DeadLetter, 0, len(msgs))
	for _, m := range msgs {
		raw, _ := m.Values["record"].(string)
		d, err := domain.UnmarshalDeadLetter(raw)
		if err != nil {
			return out, fmt.Errorf("dead letter entry %s: %w", m.ID, err)
		}
		out = append(out, d)
	}
	return out, nil
}

// DeadLetterCount returns the number of dead-lettered tasks.
func (b *Broker) DeadLetterCount(ctx context.Context) (int64, error) {
	n, err := b.rdb.XLen(ctx, b.keys.DeadLetter).Result()
	if err != nil {
		return 0, fmt.Errorf("dead letter length: %w", err)
	}
	return n, nil
}

// ResultCount returns the number of terminal result records.
func (b *Broker) ResultCount(ctx context.Context) (int64, error) {
	n, err := b.rdb.XLen(ctx, b.keys.Results).Result()
	if err != nil {
		return 0, fmt.Errorf("results length: %w", err)
	}
	return n, nil
}

// ExecPending returns the number of exec-stream entries delivered to workers but
// not yet acknowledged.
func (b *Broker) ExecPending(ctx context.Context) (int64, error) {
	res, err := b.rdb.XPending(ctx, b.keys.Exec, b.keys.WorkerGroup).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("exec pending: %w", err)
	}
	return res.Count, nil
}

// Known reports, for each task ID, whether the broker has already seen it,
// either as a pending/in-flight payload or as a terminal completion. The
// scheduler uses it to skip re-ranking ingress entries that were redelivered
// after a crash, which would otherwise charge a WFQ tenant twice for the same
// task. Correctness does not depend on this check; the admit script is the
// authoritative guard.
func (b *Broker) Known(ctx context.Context, ids []string) (map[string]bool, error) {
	out := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	fields := make([]string, len(ids))
	copy(fields, ids)

	pipe := b.rdb.Pipeline()
	payloads := pipe.HMGet(ctx, b.keys.Payloads, fields...)
	done := pipe.HMGet(ctx, b.keys.Done, fields...)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("known lookup: %w", err)
	}
	p, err := payloads.Result()
	if err != nil {
		return nil, fmt.Errorf("known lookup payloads: %w", err)
	}
	d, err := done.Result()
	if err != nil {
		return nil, fmt.Errorf("known lookup done: %w", err)
	}
	for i, id := range ids {
		if (i < len(p) && p[i] != nil) || (i < len(d) && d[i] != nil) {
			out[id] = true
		}
	}
	return out, nil
}
