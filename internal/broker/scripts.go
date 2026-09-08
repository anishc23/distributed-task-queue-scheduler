package broker

import "github.com/redis/go-redis/v9"

// The Lua scripts below are the correctness core of the system. Each one runs
// atomically inside Redis, which is what makes the hand-offs between ingress,
// scheduling, dispatch, completion and retry safe across process crashes.
//
// All scripts declare every key they touch and never build key names from data,
// so they behave identically under a Redis proxy that inspects KEYS.

// admitScript moves a task from the ingress stream into the persistent pending
// set. It is idempotent: if the same ingress entry is redelivered because the
// scheduler crashed before acknowledging, the task is not admitted twice.
//
// KEYS: payloads, pending, pendingAge, done, stats
// ARGV: taskID, member, score, submittedAtMillis, payloadJSON
// Returns 1 when admitted, 0 when the task was already known.
var admitScript = redis.NewScript(`
local payloads   = KEYS[1]
local pending    = KEYS[2]
local pendingAge = KEYS[3]
local done       = KEYS[4]
local stats      = KEYS[5]

local taskID    = ARGV[1]
local member    = ARGV[2]
local score     = ARGV[3]
local submitted = ARGV[4]
local payload   = ARGV[5]

if redis.call('HEXISTS', done, taskID) == 1 then
  return 0
end
if redis.call('HEXISTS', payloads, taskID) == 1 then
  return 0
end

redis.call('HSET', payloads, taskID, payload)
redis.call('ZADD', pending, score, member)
redis.call('ZADD', pendingAge, submitted, member)
redis.call('HINCRBY', stats, 'admitted', 1)
return 1
`)

// dispatchScript atomically selects the highest ranked pending tasks and pushes
// them onto the worker execution stream. Selection (ZPOPMIN) and delivery
// (XADD) happen in the same atomic unit, so a task can never be lost in the gap
// between "the scheduler chose it" and "a worker can see it".
//
// KEYS: pending, pendingAge, payloads, execStream, stats, inflight
// ARGV: maxCount, maxInFlight (0 = unlimited), nowMillis, maxLen (0 = no trim)
// Returns a flat array of {member, score, payload} triples that were dispatched.
var dispatchScript = redis.NewScript(`
local pending    = KEYS[1]
local pendingAge = KEYS[2]
local payloads   = KEYS[3]
local execStream = KEYS[4]
local stats      = KEYS[5]
local inflight   = KEYS[6]

local maxCount   = tonumber(ARGV[1])
local maxInFlight= tonumber(ARGV[2])
local nowMillis  = ARGV[3]
local maxLen     = tonumber(ARGV[4])

local out = {}
local n = 0

for i = 1, maxCount do
  if maxInFlight > 0 then
    local cur = tonumber(redis.call('GET', inflight) or '0')
    if cur >= maxInFlight then break end
  end

  local popped = redis.call('ZPOPMIN', pending, 1)
  if not popped or #popped == 0 then break end

  local member = popped[1]
  local score  = popped[2]
  redis.call('ZREM', pendingAge, member)

  local sep = string.find(member, '|', 1, true)
  if sep then
    local taskID = string.sub(member, sep + 1)
    local payload = redis.call('HGET', payloads, taskID)
    if payload then
      if maxLen > 0 then
        redis.call('XADD', execStream, 'MAXLEN', '~', maxLen, '*',
                   'task', payload, 'dispatched_at_ms', nowMillis)
      else
        redis.call('XADD', execStream, '*',
                   'task', payload, 'dispatched_at_ms', nowMillis)
      end
      redis.call('HINCRBY', stats, 'dispatched', 1)
      redis.call('INCR', inflight)
      out[n + 1] = member
      out[n + 2] = score
      out[n + 3] = payload
      n = n + 3
    end
  end
end

return out
`)

// completeScript records a terminal successful completion exactly once and
// acknowledges the worker stream entry in the same atomic step.
//
// Idempotency: HSETNX on the done hash is the single source of truth. If the
// same task is delivered twice (which at-least-once delivery permits), the
// second completion increments a duplicate counter, writes no result record,
// and still acknowledges the redundant delivery.
//
// KEYS: done, payloads, resultsStream, stats, tenantService, tenantCount, inflight, execStream
// ARGV: taskID, resultJSON, tenantID, serviceMillis, missedDeadline(0|1), nowMillis, workerGroup, execMessageID, maxLen
// Returns 1 when the completion was recorded, 0 when it was a duplicate.
var completeScript = redis.NewScript(`
local done          = KEYS[1]
local payloads      = KEYS[2]
local results       = KEYS[3]
local stats         = KEYS[4]
local tenantService = KEYS[5]
local tenantCount   = KEYS[6]
local inflight      = KEYS[7]
local execStream    = KEYS[8]

local taskID    = ARGV[1]
local resultJSON= ARGV[2]
local tenantID  = ARGV[3]
local serviceMS = tonumber(ARGV[4])
local missed    = tonumber(ARGV[5])
local nowMillis = ARGV[6]
local group     = ARGV[7]
local msgID     = ARGV[8]
local maxLen    = tonumber(ARGV[9])

local recorded = 0
if redis.call('HSETNX', done, taskID, nowMillis) == 1 then
  redis.call('HDEL', payloads, taskID)
  if maxLen > 0 then
    redis.call('XADD', results, 'MAXLEN', '~', maxLen, '*', 'result', resultJSON)
  else
    redis.call('XADD', results, '*', 'result', resultJSON)
  end
  redis.call('HINCRBY', stats, 'completed', 1)
  if missed == 1 then
    redis.call('HINCRBY', stats, 'deadline_missed', 1)
  end
  redis.call('HINCRBY', tenantService, tenantID, serviceMS)
  redis.call('HINCRBY', tenantCount, tenantID, 1)
  redis.call('DECR', inflight)
  recorded = 1
else
  redis.call('HINCRBY', stats, 'duplicate_completions', 1)
end

if msgID ~= '' then
  redis.call('XACK', execStream, group, msgID)
end
return recorded
`)

// retryScript redelivers a failed or lost task, or retires it to the
// dead-letter stream when its retry budget is exhausted. The acknowledgement of
// the old delivery happens in the same atomic step as the new delivery, so a
// crash cannot duplicate the retry or drop the task.
//
// KEYS: done, payloads, execStream, deadStream, stats, inflight
// ARGV: taskID, retryPayloadJSON, deadRecordJSON, isDead(0|1), nowMillis, group, oldMessageID, maxLen
// Returns 0 when the task was already terminal, 1 when retried, 2 when dead-lettered.
var retryScript = redis.NewScript(`
local done       = KEYS[1]
local payloads   = KEYS[2]
local execStream = KEYS[3]
local deadStream = KEYS[4]
local stats      = KEYS[5]
local inflight   = KEYS[6]

local taskID     = ARGV[1]
local retryJSON  = ARGV[2]
local deadJSON   = ARGV[3]
local isDead     = tonumber(ARGV[4])
local nowMillis  = ARGV[5]
local group      = ARGV[6]
local oldMsgID   = ARGV[7]
local maxLen     = tonumber(ARGV[8])

local function ackOld()
  if oldMsgID ~= '' then
    redis.call('XACK', execStream, group, oldMsgID)
  end
end

if redis.call('HEXISTS', done, taskID) == 1 then
  ackOld()
  return 0
end

if isDead == 1 then
  redis.call('XADD', deadStream, '*', 'record', deadJSON)
  redis.call('HDEL', payloads, taskID)
  redis.call('HSETNX', done, taskID, nowMillis)
  redis.call('HINCRBY', stats, 'dead_lettered', 1)
  redis.call('DECR', inflight)
  ackOld()
  return 2
end

redis.call('HSET', payloads, taskID, retryJSON)
if maxLen > 0 then
  redis.call('XADD', execStream, 'MAXLEN', '~', maxLen, '*',
             'task', retryJSON, 'dispatched_at_ms', nowMillis)
else
  redis.call('XADD', execStream, '*', 'task', retryJSON, 'dispatched_at_ms', nowMillis)
end
redis.call('HINCRBY', stats, 'retried', 1)
ackOld()
return 1
`)
