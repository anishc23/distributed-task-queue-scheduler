# Distributed Task Queue with Pluggable Scheduling
#
# Run `make help` for the full list of targets.

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ---- configuration ---------------------------------------------------------
GO              ?= go
PYTHON          ?= python3
VENV            ?= .venv
VENV_PY         := $(VENV)/bin/python
BIN             := bin
CONFIG          ?= configs/default.yaml
REDIS_ADDR      ?= localhost:6379
NAMESPACE       ?=
SCHEDULER       ?= fifo
WORKLOAD        ?= uniform
COUNT           ?= 500
SEED            ?= 42
WORKERS         ?= 2
CONCURRENCY     ?= 4
REPETITIONS     ?= 1
RUN             ?=
RESULTS_DIR     ?= results
RUN_CONFIG      ?= experiments/quick.yaml
KIND_CLUSTER    ?= taskqueue
IMAGE           ?= distributed-task-queue:dev
WORKER_REPLICAS ?= 2

# Flags shared by every Go command.
COMMON_FLAGS := --config=$(CONFIG) --redis-addr=$(REDIS_ADDR)
ifneq ($(NAMESPACE),)
COMMON_FLAGS += --namespace=$(NAMESPACE)
endif

GOFILES := $(shell find . -name '*.go' -not -path './$(VENV)/*' 2>/dev/null)

.PHONY: help
help: ## Show this help
	@echo "Distributed Task Queue with Pluggable Scheduling"
	@echo ""
	@echo "Usage: make <target> [VAR=value ...]"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} \
	     /^## ---/ {gsub(/^## --- | ---$$/, ""); printf "\n\033[1m%s\033[0m\n", $$0} \
	     /^[a-zA-Z0-9_.-]+:.*?## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Common variables:"
	@echo "  CONFIG=$(CONFIG)  SCHEDULER=$(SCHEDULER)  WORKLOAD=$(WORKLOAD)"
	@echo "  COUNT=$(COUNT)  WORKERS=$(WORKERS)  CONCURRENCY=$(CONCURRENCY)  REPETITIONS=$(REPETITIONS)"
	@echo "  REDIS_ADDR=$(REDIS_ADDR)  RESULTS_DIR=$(RESULTS_DIR)"

## --- Build ---

.PHONY: build
build: ## Build every binary into ./bin
	@mkdir -p $(BIN)
	$(GO) build -trimpath -o $(BIN)/scheduler ./cmd/scheduler
	$(GO) build -trimpath -o $(BIN)/worker    ./cmd/worker
	$(GO) build -trimpath -o $(BIN)/producer  ./cmd/producer
	$(GO) build -trimpath -o $(BIN)/benchmark ./cmd/benchmark
	$(GO) build -trimpath -o $(BIN)/tqctl     ./cmd/tqctl
	@echo "binaries in ./$(BIN)"

.PHONY: tidy
tidy: ## Sync go.mod and go.sum
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove build artefacts (results are left alone)
	rm -rf $(BIN)
	$(GO) clean -testcache

.PHONY: clean-results
clean-results: ## Delete every generated benchmark result
	rm -rf $(RESULTS_DIR)/*/
	@echo "cleared $(RESULTS_DIR)/"

## --- Quality ---

.PHONY: fmt
fmt: ## Rewrite Go source with gofmt
	gofmt -w $(GOFILES)

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt-clean
	@unformatted=$$(gofmt -l $(GOFILES)); \
	if [ -n "$$unformatted" ]; then \
		echo "these files are not gofmt-clean:"; echo "$$unformatted"; \
		echo "run 'make fmt'"; exit 1; \
	fi
	@echo "gofmt: clean"

.PHONY: vet
vet: ## Run go vet, including integration-tagged files
	$(GO) vet ./...
	$(GO) vet -tags integration ./...

.PHONY: check
check: fmt-check vet test ## Formatting, vet and unit tests

.PHONY: verify
verify: fmt-check vet test test-integration ## Everything check does, plus integration tests

## --- Tests ---

.PHONY: test
test: ## Unit tests only; needs no external services
	$(GO) test -race -count=1 ./...

.PHONY: test-short
test-short: ## Unit tests without the race detector
	$(GO) test -count=1 ./...

.PHONY: test-integration
test-integration: ## Integration tests against a real Redis (see redis-up)
	@$(MAKE) --no-print-directory redis-check
	TQ_TEST_REDIS_ADDR=$(REDIS_ADDR) $(GO) test -tags integration -race -count=1 -timeout 15m ./...

.PHONY: test-all
test-all: test test-integration ## Unit and integration tests

.PHONY: cover
cover: ## Unit test coverage summary
	$(GO) test -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@echo "HTML report: go tool cover -html=coverage.out"

.PHONY: cover-all
cover-all: ## Combined unit and integration coverage over internal/ (needs Redis)
	@$(MAKE) --no-print-directory redis-check
	TQ_TEST_REDIS_ADDR=$(REDIS_ADDR) $(GO) test -tags integration -count=1 \
		-coverpkg=./internal/... -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@echo "HTML report: go tool cover -html=coverage.out"

## --- Local services ---

.PHONY: redis-up
redis-up: ## Start Redis via Docker Compose
	docker compose up -d redis
	@echo "Redis listening on $(REDIS_ADDR)"

.PHONY: redis-down
redis-down: ## Stop the Redis container
	docker compose stop redis

.PHONY: redis-cli
redis-cli: ## Open a redis-cli session against the running Redis
	docker compose exec redis redis-cli

.PHONY: redis-check
redis-check: ## Fail with instructions if Redis is unreachable
	@if command -v redis-cli >/dev/null 2>&1; then \
		redis-cli -u "redis://$(REDIS_ADDR)" ping >/dev/null 2>&1 || { \
			echo "Redis is not reachable at $(REDIS_ADDR)."; \
			echo "Start it with:  make redis-up"; exit 1; }; \
	else \
		docker compose exec -T redis redis-cli ping >/dev/null 2>&1 || { \
			echo "Redis is not reachable at $(REDIS_ADDR)."; \
			echo "Start it with:  make redis-up"; exit 1; }; \
	fi
	@echo "Redis is reachable at $(REDIS_ADDR)"

.PHONY: up
up: ## Start the whole stack: Redis, scheduler, workers, Prometheus
	SCHEDULER_POLICY=$(SCHEDULER) WORKER_REPLICAS=$(WORKERS) WORKER_CONCURRENCY=$(CONCURRENCY) \
		docker compose up -d --build
	@echo ""
	@echo "scheduler metrics : http://localhost:9101/metrics"
	@echo "prometheus        : http://localhost:9090"
	@echo "submit a workload : make submit"

.PHONY: down
down: ## Stop the stack and remove its volumes
	docker compose --profile tools down -v

.PHONY: logs
logs: ## Follow stack logs
	docker compose logs -f --tail=100

.PHONY: submit
submit: ## Run the producer once against the running Compose stack
	WORKLOAD=$(WORKLOAD) TASK_COUNT=$(COUNT) docker compose run --rm producer

.PHONY: scale-workers
scale-workers: ## Scale Compose workers: make scale-workers WORKERS=6
	docker compose up -d --scale worker=$(WORKERS) --no-recreate worker
	@echo "worker replicas: $(WORKERS)"

## --- Run components locally ---

.PHONY: run-scheduler
run-scheduler: ## Run a scheduler locally: make run-scheduler SCHEDULER=wfq
	$(GO) run ./cmd/scheduler $(COMMON_FLAGS) --policy=$(SCHEDULER)

.PHONY: run-worker
run-worker: ## Run a worker locally: make run-worker CONCURRENCY=8
	$(GO) run ./cmd/worker $(COMMON_FLAGS) --concurrency=$(CONCURRENCY)

.PHONY: run-worker-flaky
run-worker-flaky: ## Run a worker that abandons 30% of tasks before acknowledging
	$(GO) run ./cmd/worker $(COMMON_FLAGS) --concurrency=$(CONCURRENCY) \
		--fail-before-ack-rate=0.3 --metrics-addr=:9103 --name=flaky

.PHONY: run-producer
run-producer: ## Submit a workload locally: make run-producer WORKLOAD=bursty COUNT=1000
	$(GO) run ./cmd/producer $(COMMON_FLAGS) \
		--workload=$(WORKLOAD) --count=$(COUNT) --seed=$(SEED)

.PHONY: status
status: ## Show live queue state, counters and per-tenant service
	$(GO) run ./cmd/tqctl status $(COMMON_FLAGS)

.PHONY: dlq
dlq: ## List dead-letter entries with failure metadata
	$(GO) run ./cmd/tqctl dlq $(COMMON_FLAGS)

.PHONY: reset
reset: ## Delete every Redis key in the configured namespace
	$(GO) run ./cmd/tqctl reset $(COMMON_FLAGS) --yes

.PHONY: metrics
metrics: ## Print the project's own metrics from the local scheduler endpoint
	@curl -s http://localhost:9101/metrics | grep '^tq_' || \
		echo "no scheduler metrics endpoint on :9101; is the scheduler running?"

## --- Benchmarks ---

.PHONY: bench
bench: ## Run the full 4x4 matrix: make bench RUN_CONFIG=experiments/full.yaml REPETITIONS=3
	@$(MAKE) --no-print-directory redis-check
	$(GO) run ./cmd/benchmark --config=$(RUN_CONFIG) --redis-addr=$(REDIS_ADDR) \
		--repetitions=$(REPETITIONS) --workers=$(WORKERS) --concurrency=$(CONCURRENCY) \
		--results-dir=$(RESULTS_DIR) $(if $(RUN),--run-id=$(RUN),)

.PHONY: bench-quick
bench-quick: ## Fast 16-experiment smoke run of the whole matrix
	@$(MAKE) --no-print-directory redis-check
	$(GO) run ./cmd/benchmark --config=experiments/quick.yaml --redis-addr=$(REDIS_ADDR) \
		--repetitions=1 --workers=2 --concurrency=4 --results-dir=$(RESULTS_DIR) \
		$(if $(RUN),--run-id=$(RUN),)

.PHONY: bench-full
bench-full: ## Long run suitable for reportable results (3 repetitions)
	@$(MAKE) --no-print-directory redis-check
	$(GO) run ./cmd/benchmark --config=experiments/full.yaml --redis-addr=$(REDIS_ADDR) \
		--repetitions=$(REPETITIONS) --workers=4 --concurrency=4 --results-dir=$(RESULTS_DIR) \
		$(if $(RUN),--run-id=$(RUN),)

.PHONY: bench-failure
bench-failure: ## Matrix with failure injection, exercising retries and dead-lettering
	@$(MAKE) --no-print-directory redis-check
	$(GO) run ./cmd/benchmark --config=experiments/failure.yaml --redis-addr=$(REDIS_ADDR) \
		--repetitions=1 --workers=2 --concurrency=4 --results-dir=$(RESULTS_DIR) \
		$(if $(RUN),--run-id=$(RUN),)

.PHONY: bench-one
bench-one: ## Single cell: make bench-one SCHEDULER=wfq WORKLOAD=multi_tenant
	@$(MAKE) --no-print-directory redis-check
	$(GO) run ./cmd/benchmark --config=$(RUN_CONFIG) --redis-addr=$(REDIS_ADDR) \
		--schedulers=$(SCHEDULER) --workloads=$(WORKLOAD) --repetitions=$(REPETITIONS) \
		--workers=$(WORKERS) --concurrency=$(CONCURRENCY) --results-dir=$(RESULTS_DIR) \
		$(if $(RUN),--run-id=$(RUN),)

## --- Analysis ---

.PHONY: python-deps
python-deps: ## Create a virtualenv and install pandas, matplotlib and seaborn
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --quiet --upgrade pip
	$(VENV)/bin/pip install --quiet -r requirements.txt
	@echo "analysis dependencies installed in $(VENV)"

.PHONY: plots
plots: ## Generate charts: make plots RUN=<run id> (defaults to the newest run)
	@test -x $(VENV_PY) || { echo "run 'make python-deps' first"; exit 1; }
	$(VENV_PY) scripts/plot_results.py --results-dir=$(RESULTS_DIR) $(if $(RUN),--run=$(RUN),)

.PHONY: merge
merge: ## Combine runs into one analysable directory: make merge OUT=final RUNS="a b c"
	@test -n "$(RUNS)" || { echo 'set RUNS="run1 run2 ..."'; exit 1; }
	@test -x $(VENV_PY) || { echo "run 'make python-deps' first"; exit 1; }
	$(VENV_PY) scripts/merge_runs.py --results-dir=$(RESULTS_DIR) --out=$(or $(OUT),merged) $(RUNS)

.PHONY: report
report: bench-quick plots ## Run the quick matrix and then plot it

## --- Kubernetes (kind) ---

.PHONY: kind-up
kind-up: ## Create the kind cluster, build and load the image, deploy the stack
	KIND_CLUSTER=$(KIND_CLUSTER) TQ_IMAGE=$(IMAGE) WORKER_REPLICAS=$(WORKER_REPLICAS) \
		scripts/kind-up.sh --cluster $(KIND_CLUSTER) --workers $(WORKER_REPLICAS)

.PHONY: kind-up-wfq
kind-up-wfq: ## Same as kind-up but deploys the weighted-fair-queuing overlay
	KIND_CLUSTER=$(KIND_CLUSTER) TQ_IMAGE=$(IMAGE) \
		scripts/kind-up.sh --cluster $(KIND_CLUSTER) --overlay deploy/kubernetes/overlays/wfq

.PHONY: kind-down
kind-down: ## Delete only this project's kind cluster
	scripts/kind-down.sh --cluster $(KIND_CLUSTER)

.PHONY: kind-submit
kind-submit: ## Run the producer Job in the cluster
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue delete job producer --ignore-not-found
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue apply -f deploy/kubernetes/base/producer-job.yaml
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue wait --for=condition=complete job/producer --timeout=300s

.PHONY: kind-scale
kind-scale: ## Scale workers in the cluster: make kind-scale WORKER_REPLICAS=6
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue scale deployment/worker --replicas=$(WORKER_REPLICAS)
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue rollout status deployment/worker --timeout=180s

.PHONY: kind-status
kind-status: ## Show queue state from inside the cluster
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue exec deploy/scheduler -- \
		tqctl status --config /etc/taskqueue/config.yaml

.PHONY: kind-dlq
kind-dlq: ## Inspect the dead-letter stream from inside the cluster
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue exec deploy/scheduler -- \
		tqctl dlq --config /etc/taskqueue/config.yaml

.PHONY: kind-metrics
kind-metrics: ## Port-forward the scheduler metrics endpoint to localhost:9101
	@echo "scheduler metrics on http://localhost:9101/metrics (Ctrl-C to stop)"
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue port-forward svc/scheduler 9101:9101

.PHONY: kind-prometheus
kind-prometheus: ## Port-forward Prometheus to localhost:9090
	@echo "Prometheus on http://localhost:9090 (Ctrl-C to stop)"
	kubectl --context kind-$(KIND_CLUSTER) -n taskqueue port-forward svc/prometheus 9090:9090

.PHONY: k8s-manifests
k8s-manifests: ## Render the Kubernetes manifests without applying them
	kubectl kustomize deploy/kubernetes/base

## --- Docker image ---

.PHONY: docker-build
docker-build: ## Build the container image
	docker build -f deploy/docker/Dockerfile -t $(IMAGE) .
