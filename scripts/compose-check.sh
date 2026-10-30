#!/usr/bin/env bash
# End-to-end check of the Docker Compose stack, with the live console watching.
#
#   make compose-check
#
# Brings the stack up, submits a workload with the producer container, kills
# whichever scheduler container leads while work is flowing, and checks what
# the console and Prometheus report against what must be true:
#
#   - the standby takes the lease under a newer epoch, within a crash's window
#   - the dispatch log has no epoch inversion
#   - every submitted task completes, and is recorded as completed once
#   - after the killed container is restarted, exactly one scheduler leads,
#     by the console's count and by Prometheus's, and every target is up
#   - the console, which did not start these containers, refuses to signal them
#
# It uses the Compose project in this directory and tears it down (volumes
# included) on exit, so do not run it against a stack you want to keep.

set -euo pipefail
cd "$(dirname "$0")/.."

TASKS=${TASKS:-3000}
PORT=${CONSOLE_PORT:-8098}
API="http://127.0.0.1:${PORT}/api/v1"
PROM="http://localhost:9090/api/v1"
CONSOLE_PID=""

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "FAIL: $*"; exit 1; }

cleanup() {
  [ -n "$CONSOLE_PID" ] && kill "$CONSOLE_PID" 2>/dev/null || true
  docker compose --profile tools down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# json <url> <python expression over d>: fetch and evaluate.
json() {
  curl -fsS --max-time 5 "$1" | python3 -c "import json,sys; d=json.load(sys.stdin); print($2)"
}

# wait_for <seconds> <description> <command...>: poll until the command succeeds.
wait_for() {
  local limit=$1 what=$2; shift 2
  local deadline=$((SECONDS + limit))
  until "$@" >/dev/null 2>&1; do
    [ $SECONDS -ge $deadline ] && fail "timed out after ${limit}s waiting for ${what}"
    sleep 1
  done
}

log "building the console and starting the stack"
go build -o bin/console ./cmd/console
docker compose --profile tools down -v >/dev/null 2>&1 || true
docker compose up -d --build --wait >/dev/null

./bin/console --redis-addr localhost:6379 --listen "127.0.0.1:${PORT}" --static "" --log-level warn &
CONSOLE_PID=$!

one_leader() { [ "$(json "$API/status" 'd["snapshot"]["leader_sum"]')" = "1" ]; }
wait_for 30 "the console to see exactly one leader" one_leader

leader_service() {
  json "$API/status" '[c["name"] for c in d["snapshot"]["components"] if (c.get("metrics") or {}).get("tq_scheduler_is_leader") == 1][0]'
}
epoch() { json "$API/status" 'd["snapshot"]["leadership"].get("epoch", 0)'; }

log "submitting ${TASKS} tasks"
WORKLOAD=uniform TASK_COUNT=$TASKS docker compose run --rm producer >/dev/null 2>&1 &
PRODUCER=$!
dispatching() { [ "$(json "$API/status" 'd["snapshot"]["queue"]["counters"].get("dispatched", 0)')" -gt 300 ]; }
wait_for 60 "dispatching to start" dispatching

LEADER=$(leader_service)
BEFORE=$(epoch)
KILLED_AT=$(python3 -c 'import time; print(int(time.time() * 1000))')
log "killing the leading container (${LEADER}, epoch ${BEFORE})"
docker compose kill "$LEADER" >/dev/null 2>&1

newer() { [ "$(epoch)" -gt "$BEFORE" ]; }
wait_for 20 "a standby to take the lease" newer
log "lease taken under epoch $(epoch)"

sleep 3
read -r GAP GAP_END INVERSIONS < <(json "$API/dispatches?window_s=60" 'd.get("max_gap_ms", 0), d.get("max_gap_end_ms", 0), d["epoch_inversions"]' | tr -d '(),')
[ "$INVERSIONS" = "0" ] || fail "${INVERSIONS} epoch inversions in the dispatch log"
[ "$GAP_END" -gt "$KILLED_AT" ] || fail "the longest dispatch gap is not the one after the kill"
# A crash costs at most the lease TTL plus one standby poll (5 s + 1 s with
# the shipped settings); allow a margin for a loaded CI runner.
[ "$GAP" -lt 10000 ] || fail "dispatch stopped for ${GAP} ms"
log "dispatch stopped for ${GAP} ms; no epoch inversions"

log "restarting ${LEADER}"
docker compose start "$LEADER" >/dev/null 2>&1
wait "$PRODUCER" || fail "the producer failed"

drained() {
  json "$API/status" 'd["snapshot"]["queue"]["counters"].get("completed", 0) == '"$TASKS"' and d["snapshot"]["queue"]["pending"] == 0 and d["snapshot"]["queue"]["in_flight"] == 0 and d["snapshot"]["queue"]["not_admitted"] == 0' | grep -qx True
}
wait_for 120 "all ${TASKS} tasks to complete" drained
DEAD=$(json "$API/status" 'd["snapshot"]["queue"]["dead_lettered"]')
[ "$DEAD" = "0" ] || fail "${DEAD} tasks were dead-lettered"
log "all ${TASKS} tasks completed, each recorded once; none dead-lettered"

wait_for 30 "the console to see exactly one leader again" one_leader
prom_one() { [ "$(json "$PROM/query?query=sum(tq_scheduler_is_leader)" 'd["data"]["result"][0]["value"][1]')" = "1" ]; }
prom_up() { json "$PROM/targets" 'all(t["health"] == "up" for t in d["data"]["activeTargets"])' | grep -qx True; }
wait_for 60 "Prometheus to count exactly one leader" prom_one
wait_for 60 "every Prometheus target to be up" prom_up
log "exactly one leader, by the console and by Prometheus; every target up"

CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"action":"kill","target":"leader"}' "$API/faults")
[ "$CODE" = "403" ] || fail "the monitor-mode console answered ${CODE} to a kill request, not 403"
log "the console refused to signal containers it did not start"

log "PASS"
