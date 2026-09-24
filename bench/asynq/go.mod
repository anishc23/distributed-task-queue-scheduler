// A separate module on purpose.
//
// This benchmark needs a third-party task queue to compare against, and that
// dependency has no business in the graph of the library this repository
// ships. Keeping it here means `go build ./...` at the root never pulls Asynq
// in, and nothing a consumer of the main module imports can reach it.
module github.com/anishc23/distributed-task-queue/bench/asynq

go 1.23.0

toolchain go1.23.12

// The dependency points one way only: this benchmark uses the queue, the queue
// never uses this benchmark.
replace github.com/anishc23/distributed-task-queue => ../..

require (
	github.com/anishc23/distributed-task-queue v0.0.0-00010101000000-000000000000
	github.com/hibiken/asynq v0.24.1
	github.com/redis/go-redis/v9 v9.7.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
