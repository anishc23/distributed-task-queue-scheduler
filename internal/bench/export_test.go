package bench

import "github.com/anishc23/distributed-task-queue/internal/config"

// LoadSuffix is exported to the test package so the naming rule that keeps a
// plain run's layout stable, and a sweep's filenames distinct, can be asserted
// directly rather than inferred from files on disk.
var LoadSuffix = loadSuffix

// ExperimentConfig exposes the per-experiment configuration derivation so the
// control arm's stream wiring can be asserted directly rather than inferred
// from timing numbers.
func (r *Runner) ExperimentConfig(id RunIdentity) *config.Config { return r.experimentConfig(id) }
