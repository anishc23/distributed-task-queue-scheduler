package policy

import (
	"container/heap"

	"github.com/anishc23/distributed-task-queue/internal/domain"
)

// entry couples a pending task with the key its policy assigned.
type entry struct {
	key  Key
	task domain.Task
}

type entryHeap []entry

func (h entryHeap) Len() int           { return len(h) }
func (h entryHeap) Less(i, j int) bool { return h[i].key.Less(h[j].key) }
func (h entryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)        { *h = append(*h, x.(entry)) }
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// Queue is an in-memory pending set driven by a Policy. It is the reference
// implementation of "choose the next task from the pending tasks" and is what
// the scheduler unit tests exercise. The production engine keeps the same
// pending set in Redis but uses the identical (Score, Member) ordering.
type Queue struct {
	policy Policy
	h      entryHeap
}

// NewQueue returns a pending queue ordered by p.
func NewQueue(p Policy) *Queue {
	q := &Queue{policy: p}
	heap.Init(&q.h)
	return q
}

// Policy returns the underlying scheduling policy.
func (q *Queue) Policy() Policy { return q.policy }

// Admit ranks a task and adds it to the pending set.
func (q *Queue) Admit(t domain.Task) Key {
	k := q.policy.Rank(t)
	heap.Push(&q.h, entry{key: k, task: t})
	return k
}

// Next removes and returns the task that should execute next. The second
// return value is false when no task is pending.
func (q *Queue) Next() (domain.Task, bool) {
	if q.h.Len() == 0 {
		return domain.Task{}, false
	}
	it := heap.Pop(&q.h).(entry)
	q.policy.Notify(it.key)
	return it.task, true
}

// Drain repeatedly calls Next until the queue is empty and returns the tasks in
// dispatch order. Useful for ordering assertions in tests.
func (q *Queue) Drain() []domain.Task {
	out := make([]domain.Task, 0, q.h.Len())
	for {
		t, ok := q.Next()
		if !ok {
			return out
		}
		out = append(out, t)
	}
}

// Pending reports how many tasks are waiting.
func (q *Queue) Pending() int { return q.h.Len() }
