package config

// Task execution modes.
//
// Every benchmark in this project defaults to sleeping for a task's declared
// duration. That makes task sizes exactly controllable and removes application
// variance from a comparison between scheduling policies, but a sleeping worker
// uses no CPU: N slots always deliver N-way parallelism regardless of how many
// cores exist, and utilisation measures slot occupancy rather than work.
//
// ExecModeCPU consumes the duration in real computation instead, so slots
// contend for cores as real tasks would. It exists to test whether conclusions
// drawn from the sleep model survive contact with actual work.
const (
	// ExecModeSleep occupies a slot for the task duration without using CPU.
	ExecModeSleep = "sleep"
	// ExecModeCPU busy-computes for approximately the task duration.
	ExecModeCPU = "cpu"
)

// ExecModes lists the supported execution modes.
func ExecModes() []string { return []string{ExecModeSleep, ExecModeCPU} }

// ValidExecMode reports whether mode is a known execution mode.
func ValidExecMode(mode string) bool {
	return mode == ExecModeSleep || mode == ExecModeCPU
}
