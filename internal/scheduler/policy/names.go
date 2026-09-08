package policy

import "sort"

// Canonical scheduling policy names. They live here rather than in the
// individual algorithm packages so that configuration validation can depend on
// the list without importing every implementation.
const (
	FIFO     = "fifo"
	Priority = "priority"
	EDF      = "edf"
	WFQ      = "wfq"
)

// Names lists every available scheduling policy, sorted.
func Names() []string {
	names := []string{FIFO, Priority, EDF, WFQ}
	sort.Strings(names)
	return names
}

// Valid reports whether name identifies a known scheduling policy.
func Valid(name string) bool {
	for _, n := range Names() {
		if n == name {
			return true
		}
	}
	return false
}
