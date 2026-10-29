package console

import (
	"sort"
	"time"

	"github.com/anishc23/distributed-task-queue/internal/broker"
)

// DispatchBucket counts the dispatches made under one epoch in one time slot.
type DispatchBucket struct {
	TMS   int64 `json:"t_ms"`
	Epoch int64 `json:"epoch"`
	N     int   `json:"n"`
}

// DispatchGap is an interval with no dispatch at all. Whether it was an
// outage or an idle queue is not knowable from the stream alone; the
// interface decides that from the pending count it sampled meanwhile.
type DispatchGap struct {
	StartMS int64 `json:"start_ms"`
	EndMS   int64 `json:"end_ms"`
}

// MinReportedGap is the shortest gap listed in Gaps.
const MinReportedGap = time.Second

// maxReportedGaps bounds the list; the longest are kept.
const maxReportedGaps = 20

// DispatchWindow summarises the execution stream over a recent window. It is
// read from Redis, from the stamps the dispatch script wrote, so it describes
// what was actually dispatched, not what any scheduler believes it did.
type DispatchWindow struct {
	ReadAt    time.Time        `json:"read_at"`
	FromMS    int64            `json:"from_ms"`
	WindowS   int              `json:"window_s"`
	BucketMS  int64            `json:"bucket_ms"`
	Count     int              `json:"count"`
	Truncated bool             `json:"truncated"`
	Buckets   []DispatchBucket `json:"buckets"`
	Epochs    []int64          `json:"epochs"`
	// Unstamped counts entries with no epoch: retries re-queued by workers,
	// which no leadership term dispatched.
	Unstamped int `json:"unstamped"`
	// MaxGapMS is the longest interval between consecutive dispatches in the
	// window, ending at MaxGapEndMS. It is the same measurement the failover
	// experiments report as the outage, taken live. It is also what an idle
	// queue looks like, which is why the interface shows it beside the backlog.
	MaxGapMS    *int64 `json:"max_gap_ms,omitempty"`
	MaxGapEndMS *int64 `json:"max_gap_end_ms,omitempty"`
	LastMS      *int64 `json:"last_dispatch_ms,omitempty"`
	// Gaps lists every gap of at least MinReportedGap, oldest first (the
	// longest maxReportedGaps if there are more).
	Gaps []DispatchGap `json:"gaps"`
	// EpochInversions counts stamped entries whose epoch is older than one
	// already written earlier in the stream: a superseded leader writing
	// after its successor. The fence exists to keep this at zero.
	EpochInversions int `json:"epoch_inversions"`
}

// SummariseDispatches buckets entries (in stream order) by time and epoch.
func SummariseDispatches(msgs []broker.ExecMessage, from time.Time, window time.Duration, bucket time.Duration, cap int) DispatchWindow {
	out := DispatchWindow{
		FromMS: from.UnixMilli(), WindowS: int(window / time.Second), BucketMS: bucket.Milliseconds(),
		Count: len(msgs), Truncated: cap > 0 && len(msgs) >= cap,
		Buckets: []DispatchBucket{}, Epochs: []int64{}, Gaps: []DispatchGap{},
	}
	type key struct {
		t     int64
		epoch int64
	}
	counts := map[key]int{}
	epochs := map[int64]bool{}
	var highest int64
	var prev int64
	for _, m := range msgs {
		if m.DispatchedAt.IsZero() {
			continue
		}
		at := m.DispatchedAt.UnixMilli()
		slot := at - (at-out.FromMS)%out.BucketMS
		counts[key{slot, m.Epoch}]++
		if m.Epoch > 0 {
			epochs[m.Epoch] = true
			if m.Epoch < highest {
				out.EpochInversions++
			}
			highest = max(highest, m.Epoch)
		} else {
			out.Unstamped++
		}
		if prev != 0 {
			gap := at - prev
			if out.MaxGapMS == nil || gap > *out.MaxGapMS {
				g, end := gap, at
				out.MaxGapMS, out.MaxGapEndMS = &g, &end
			}
			if gap >= MinReportedGap.Milliseconds() {
				out.Gaps = append(out.Gaps, DispatchGap{StartMS: prev, EndMS: at})
			}
		}
		prev = at
	}
	if prev != 0 {
		last := prev
		out.LastMS = &last
	}
	for k, n := range counts {
		out.Buckets = append(out.Buckets, DispatchBucket{TMS: k.t, Epoch: k.epoch, N: n})
	}
	sort.Slice(out.Buckets, func(i, j int) bool {
		if out.Buckets[i].TMS != out.Buckets[j].TMS {
			return out.Buckets[i].TMS < out.Buckets[j].TMS
		}
		return out.Buckets[i].Epoch < out.Buckets[j].Epoch
	})
	if len(out.Gaps) > maxReportedGaps {
		sort.Slice(out.Gaps, func(i, j int) bool {
			return out.Gaps[i].EndMS-out.Gaps[i].StartMS > out.Gaps[j].EndMS-out.Gaps[j].StartMS
		})
		out.Gaps = out.Gaps[:maxReportedGaps]
		sort.Slice(out.Gaps, func(i, j int) bool { return out.Gaps[i].StartMS < out.Gaps[j].StartMS })
	}
	for e := range epochs {
		out.Epochs = append(out.Epochs, e)
	}
	sort.Slice(out.Epochs, func(i, j int) bool { return out.Epochs[i] < out.Epochs[j] })
	return out
}
