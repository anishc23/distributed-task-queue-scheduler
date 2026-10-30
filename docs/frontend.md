# Research site and live console

`web/` is one frontend with two modes that never share a data source:

| | Research | Live system |
| --- | --- | --- |
| URL | `#overview` and the other section anchors | `#/live` |
| Shows | Published experiment results, recomputed from the raw CSVs | What a running queue is doing now |
| Data | Static JSON in `web/public/data/`, committed | The console's HTTP API on `127.0.0.1` |
| Needs | Nothing: any static host | `cmd/console` running on the same machine |
| Can change anything | No | In demo mode with `--enable-faults`, only processes the console started |

A number on the research page is a measured result with a named source file and SHA-256. A number on the live page
is current telemetry with the time it was sampled. Neither page shows the other's numbers, and the live page says
so at the top.

- [Research site](#research-site)
  - [Data pipeline](#data-pipeline)
  - [Dataset schema](#dataset-schema)
  - [Deploying it](#deploying-it)
- [Live console](#live-console)
  - [Running it](#running-it)
  - [API](#api)
  - [What each figure means](#what-each-figure-means)
  - [Security boundaries](#security-boundaries)
  - [Fault injection](#fault-injection)
  - [A ten-minute demo](#a-ten-minute-demo)
- [Tests](#tests)
- [Troubleshooting](#troubleshooting)
- [Known limitations](#known-limitations)

## Research site

Vite, React 18, TypeScript and Observable Plot, with IBM Plex served from the bundle (no third-party requests). It
is a static site: `npm run build` writes `web/dist/`, which can be served by anything.

```bash
cd web
npm ci
npm run dev        # http://127.0.0.1:5173, hot reload
npm run build      # web/dist
npm run preview    # serve web/dist on http://127.0.0.1:4173
```

`npm run dev` and `npm run build` first copy `docs/report.pdf` and `docs/images/*.png` into `web/public/docs/`
(gitignored), so the site links to the same PDF the repository holds rather than a second copy.

Layout: `src/research/` has one component per section. `src/components/` holds the shared figure, chart and control
components, and `src/lib/` the data loading, validation, statistics and palette. Every chart is wrapped in a
`Figure`, which carries its caption, the experimental conditions, the source files with their digests, the command
that produced them, and a table of the plotted numbers.

### Data pipeline

```
results/<run>/*.csv  ──scripts/export_site_data.py──▶  web/public/data/*.json  ──fetch + validate──▶  charts
   (gitignored, 1.5 GB)        recomputes 153 claims             (committed, ~560 KiB)        (src/lib/schema.ts)
```

`scripts/export_site_data.py` is the only bridge between the benchmark output and the site. It uses the Python
standard library only, and does four things:

1. Reads exactly the CSVs each dataset needs. Pooled per-task files, such as the 100 raw files behind the priority
   breakdown, are recorded as one digest over the set.
2. Writes one JSON file per dataset. Each carries the source paths, SHA-256 digests and row counts, the producing
   command, the experimental conditions and the caveats.
3. **Recomputes every number the report quotes and refuses to write anything if one disagrees.** The check found
   four errors in the report on its first run; they are recorded in report §5.1.
4. Refuses to publish an experiment listed as pending, and fails if any failover trial is flagged as suspect.

Output is deterministic: no timestamps, and re-running it on unchanged results produces no diff.

```bash
python3 scripts/export_site_data.py --check   # verify only
python3 scripts/export_site_data.py           # or: make site-data
```

To publish a new or re-run experiment, re-run it into `results/`, add or adjust its builder and claims in the
script, export, and commit the JSON. Chart components never hold numbers.

### Dataset schema

Every `web/public/data/<id>.json` has the same envelope (`schema_version` 1):

| field | meaning |
| --- | --- |
| `id`, `title`, `report_section` | what it is and where the report discusses it |
| `status` | always `published`; pending experiments are not exported, only listed in `index.json` |
| `sources[]` | `path`, `rows`, and `sha256`, or `files` and `sha256_of_set` for a pooled directory |
| `command` | the command that produced the source files |
| `conditions` | repetitions, task counts, slots and other settings that bound the result |
| `caveats[]` | what the numbers must not be read as; shown on the page beside the chart |
| `fields` | units of the measurement columns |
| `rows[]` | one object per experiment, trial or cell. Missing measurements are `null`, never `0` |

`index.json` holds the dataset list, `claims[]` (every recomputed report number with its printed value, recomputed
value, tolerance and result), `pending[]` (experiments with a harness but no publishable result, and why), and
`inventory` (experiment and task counts per run, recomputed).

`src/lib/schema.ts` validates every file at load time against a per-dataset field specification. A missing field, a
string where a number belongs, `NaN`, an unknown enum value, a dataset without provenance, or one not marked
`published` is an error. The page shows the error in place of the chart; nothing is drawn as zero.

### Deploying it

The repository includes `.github/workflows/pages.yml`, which builds the site under `/<repository>/` and publishes it
to GitHub Pages. It runs **only when started by hand**, because publishing a public page should be a deliberate
step.

1. On GitHub: **Settings → Pages → Build and deployment → Source: GitHub Actions** (one time).
2. **Actions → Publish research site → Run workflow.**
3. The site appears at `https://<user>.github.io/<repository>/`; this repository's is <https://anishc23.github.io/distributed-task-queue-scheduler/>.

Any other static host works too: `BASE=/sub/path/ npm run build` sets the path prefix, and `web/dist` is the whole
site. The research site never contacts the console, Redis or any other service.

## Live console

`cmd/console` is a small Go server. It polls a running queue and serves the same frontend's `#/live` page, plus a
JSON API. It has two modes.

**Monitor mode** watches an existing deployment and cannot change it. It reads Redis directly and scrapes the
`/healthz`, `/readyz` and `/metrics` endpoints it is given. Without `--targets` it scrapes the two schedulers the
Compose stack publishes on 9101 and 9103. Compose does not publish worker ports, so workers are not scraped there.

**Demo mode** (`--demo`) starts its own disposable cluster: two schedulers, two workers, policy `wfq`, a 5 s lease.
It runs them from `bin/` against a namespace that must start with `tq-console`, and resets that namespace on start.
Each scheduler reaches Redis through its own fault-injection proxy (`internal/netfault`), so one can be partitioned
while Redis stays up for the others. With `--enable-faults`, the page can kill, terminate, freeze, partition and
restart those processes, and submit one of three fixed workloads.

### Running it

```bash
make redis-up                 # or any Redis on localhost:6379
make console-demo             # build, then: bin/console --demo --enable-faults
```

The console prints a URL ending in `#/live?token=…`. Open that one: the token unlocks the controls. Without it the
page is read-only. Ctrl-C stops the console, which ends any freeze or partition, stops every demo process (SIGTERM,
then SIGKILL after 5 s), and closes the proxies.

| flag | default | |
| --- | --- | --- |
| `--listen` | `127.0.0.1:8088` | must be a loopback address; anything else is refused at start |
| `--static` | `web/dist` | the built frontend; empty serves the API only |
| `--poll` | `1s` | how often Redis is read and every process scraped |
| `--targets` | Compose's two schedulers | monitor mode: `name=http://host:port,…`; names starting `sched` are schedulers |
| `--demo` | off | start and supervise the demo cluster |
| `--enable-faults` | off | demo mode only: allow fault injection |
| `--schedulers`, `--workers`, `--concurrency` | 2, 2, 4 | demo cluster size (bounded to 3, 4 and 16) |
| `--policy`, `--lease-ttl`, `--lease-renew` | `wfq`, `5s`, `1.5s` | demo scheduler settings |
| `--allow-origin` | none | extra Origin allowed to send controls, e.g. `http://127.0.0.1:5173` for `npm run dev` |

To develop the live page with hot reload, run the console with `--allow-origin http://127.0.0.1:5173`, then
`npm run dev`. The dev server proxies `/api` to the console.

### API

All responses are JSON. Reads need only the right `Host` header; writes need everything in
[Security boundaries](#security-boundaries).

| endpoint | returns |
| --- | --- |
| `GET /api/v1/status` | `console` (mode, server time, poll interval, faults on/off) and the latest `snapshot` |
| `GET /api/v1/history` | the last 600 samples (10 min at 1 s): queue depth, rates, lease holder, leader count |
| `GET /api/v1/events?since=<seq>` | events after a sequence number, each tagged with its source |
| `GET /api/v1/dispatches?window_s=90` | the dispatch log over a window (10–300 s): 250 ms buckets by epoch, every gap of 1 s or more, the longest gap, epoch inversions |
| `GET /api/v1/tenants?window_s=120` | completions per tenant over a window (10–600 s), read from the results stream: count, p50 and p95 latency from submission to completion, deadline misses |
| `GET /api/v1/dlq?limit=50` | dead-letter metadata: task id, tenant, attempts, reason, time. Never the payload |
| `GET /api/v1/controls` | whether faults are on and why not, allowed actions per role, process states, workload presets |
| `POST /api/v1/faults` | `{"action": "kill" \| "terminate" \| "freeze" \| "partition" \| "restart", "target": "leader" \| "<name>", "duration_ms": 1000–30000}` |
| `POST /api/v1/faults/clear` | `{}`: ends every freeze and partition now |
| `POST /api/v1/workload` | `{"preset": "steady" \| "overload" \| "tenant-skew"}` |

Errors are `{"error": "…"}`. The status codes: 400 for a malformed or disallowed request; 401 for a missing or wrong
token; 403 for a cross-origin request, for faults that are off, or for any write in monitor mode; 409 when a request
is valid but not now (the target is already frozen, or a workload is already running); 415 for a body that is not
JSON; 421 for a foreign `Host`; 502 when Redis could not be read.

### What each figure means

| figure | source | when unavailable |
| --- | --- | --- |
| pending, in flight, dead-lettered, oldest wait | Redis, via the same broker calls as `tqctl status` | "—", and a Redis-unreachable notice |
| not yet admitted | ingress entries the scheduler group has not read, counted directly (up to 10,000), plus those read but not acknowledged. While no scheduler leads this is where new work piles up; the pending set does not move | "—" |
| dispatched/s, completed/s | difference between two consecutive readings of the Redis counters `dispatched` and `completed`, over the measured interval | "—" on the first sample, after a failed read, or after a counter reset |
| lease holder, epoch, lease expiry, fence | Redis keys `leader`, `leader_epoch`, `fence` | "unknown" |
| schedulers claiming to lead | sum of `tq_scheduler_is_leader` over schedulers | "unknown" if any scheduler could not be scraped. A process the supervisor saw exit counts as 0; a frozen one does not, because it may still believe it leads |
| health | `/healthz` and `/readyz` status codes (see below) | — |
| tenants | the results stream: every task completed in the window, by tenant; latency is finish minus submit, percentiles interpolated as in the benchmark analysis | "No task completed in this window" |
| dispatch log | the execution stream itself: every entry's stamped `dispatched_at_ms` and `epoch` | an error in place of the chart |
| stall or idle | a gap of 1 s or more in the dispatch log is a *stall* if a sample taken during it shows work waiting (pending or not yet admitted), *idle* if every sample shows nothing waiting, and *unknown* if no sample fell inside it or Redis was unreadable. Only stalls count towards "longest stall" | "unknown", never assumed |

Health follows the endpoints' own semantics (`internal/metrics`):

| state | meaning |
| --- | --- |
| `ready` | `/healthz` and `/readyz` both 200 |
| `alive, not ready` | `/healthz` 200, `/readyz` not 200: started but not yet consuming |
| `unhealthy` | `/healthz` answered with something other than 200 |
| `unreachable` | no HTTP answer at all: a dead process, a frozen one, and a closed port all look like this |

The page's own connection state is separate from all of these:

| state | meaning |
| --- | --- |
| `live` | the console answered and its newest snapshot is under three poll intervals old, measured on the console's clock |
| `stale` | the console answers but its own poll has not completed recently; values are greyed and labelled not current |
| `console not answering` | requests are failing; the last values stay on screen, greyed, with a retry countdown (1, 2, 4, 8, then 10 s) |
| `no console` | the address does not serve the console at all, such as the published site; shows how to run one |

Events carry one of three sources, because their timestamps mean different things:

| source | time is |
| --- | --- |
| `log` | the process's own, from its structured log, e.g. "became scheduler leader", "fenced out by a newer scheduler epoch" |
| `observed` | the poll that noticed a change; it happened up to one poll interval earlier |
| `control` | when the console did something on request |

### Security boundaries

A fault-injection service on a laptop is a target for any web page the laptop's browser opens. The console is built
so that only the person at that terminal can drive it:

1. **Loopback only.** `--listen` must be a loopback address. The console exits otherwise.
2. **Host check (DNS rebinding).** Every request's `Host` must be the console's own address. A page that rebinds
   its own name to 127.0.0.1 gets a 421.
3. **Session token.** Every write needs `X-Console-Token`, a random 192-bit value generated at start and printed
   only to the terminal. The console never serves it. The page reads it from the URL fragment, which browsers do
   not send to servers, moves it to `sessionStorage` (this tab only), and removes it from the address bar. Monitor
   mode has no token and accepts no writes.
4. **Same-origin JSON only.** Writes must be `application/json` with a same-origin `Origin` (plus any
   `--allow-origin`) and, where the browser sends it, `Sec-Fetch-Site: same-origin`. The console sends no CORS
   headers, so a cross-origin script fails its preflight.
5. **Fixed operations on owned processes.** A write names an action from a fixed table and a target from the
   console's own process list. No request field becomes a path, an argument or a shell command. Bodies are capped
   at 4 KiB and unknown fields are rejected.
6. **Headers.** Every response carries `Content-Security-Policy` (`default-src 'self'`, `frame-ancestors 'none'`),
   `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer` and
   `Cache-Control: no-store`.

The console does not use the Docker socket and cannot touch the Compose stack's containers.

### Fault injection

| action | applies to | what happens | ends |
| --- | --- | --- | --- |
| `kill` | scheduler, worker | `SIGKILL`; nothing is released | `restart` |
| `terminate` | scheduler | `SIGTERM`; the scheduler releases its lease on the way out | `restart` |
| `freeze` | scheduler, worker | `SIGSTOP`, confirmed from `ps`; `SIGCONT` after the duration | the duration, "End all timed faults", or console exit |
| `partition` | scheduler | its proxy to Redis is cut: open connections closed, new ones refused | the duration, "End all timed faults", or console exit |
| `restart` | an exited process | started again with the same arguments | — |

What can be lost: nothing acknowledged. A killed or frozen worker's in-flight tasks are redelivered by the recovery
loop after the visibility timeout; that is the at-least-once guarantee, and completion is still recorded once. A
killed leader's unacknowledged ingress entries are reclaimed by its successor ("reclaimed ingress entries" in the
events). The demo namespace is the only data the console touches, and it is reset at the next start.

### A ten-minute demo

The live page has this walkthrough built in; the research section each step corresponds to is in brackets.

1. Open the printed URL. Every process should be ready, with exactly one scheduler claiming to lead and the fence
   equal to the leader's epoch.
2. **Steady**: rates rise together and pending stays near zero. Below capacity there is no queue (§4.4).
3. **Overload**: pending grows; the backlog is in the scheduler's sorted set, where the policy decides who waits.
4. **Kill the leader**, which needs two clicks. The dispatch log shows the gap, and the standby's own log line
   "became scheduler leader" appears once the lease expires. Compare the gap with the 3.5–6 s window the report
   predicts and measures (§4.7). Then **restart** the killed process.
5. **Freeze the leader for 10 s**, past its 5 s lease. A standby takes over while the frozen process still believes
   it leads. On resume the events show "fenced out by a newer scheduler epoch": the epoch check refusing a live,
   superseded leader at Redis (§4.8).
6. **Partition the leader for 10 s.** The process keeps running and keeps reporting itself leader, so the claim
   count shows two. When the link heals, its first write is refused. Epoch inversions in the dispatch log stay at
   zero throughout.

7. **Optional, policy comparison.** Run the noisy-neighbour workload under `--policy wfq` and then under
   `--policy fifo` (`make console-demo POLICY=fifo`), and compare the tenants panel. On this machine, one run
   each: small tenants' p95 was about 435 ms under WFQ against tenant A's 2.8 s, and about 2.7 s, the same as A,
   under FIFO (§4.3 measures this properly).

A demo run is one observation on one machine. The measured distributions are on the research page.

#### Recording it

`make demo-video` records this walkthrough unattended, against a real demo cluster, into
`web/demo-video/demo.mp4` (macOS only). `web/scripts/record-demo.mjs` starts the console on its own port, drives
the page as a person would, and captures Chrome's own screencast. `web/scripts/encode-frames.swift` encodes the
capture to H.264 with AVFoundation; Playwright's bundled encoder is unsigned and will not run on recent macOS.

Every step waits for the system to reach the state its caption describes before going on: the new leader's own
"became scheduler leader" line, a stop confirmed from the OS process table, a fence refusal or a lost lease. If it
never arrives, the recording aborts rather than show something that did not happen. After a freeze or a partition,
the narration says whichever safe outcome actually occurred: the fence refused the stale write, or the process's
own lease renewal noticed first. Figures in captions, such as the dispatch gap and the inversion count, are read
from the page when they are shown. Captions sit in a bar labelled "narration", so they are visibly not part of the
interface.

## Tests

```bash
cd web && npm run typecheck && npm test        # 47 unit tests: schemas, real datasets, stats, connection and gap rules
cd web && npx playwright test                  # 24 browser tests against the production build, in local Chrome
go test ./internal/console/                    # console unit tests: classification, parsing, events, guards
TQ_TEST_REDIS_ADDR=localhost:6379 go test -tags integration ./internal/console/ ./internal/broker/
                                               # real binaries: kill and freeze the leader, check what is reported
CONSOLE_E2E=1 npx playwright test -g "running console"   # with make console-demo running
```

The browser tests mock the console API to reach every connection state: no console, live, stale, not answering
then recovering, Redis unreachable, two claimed leaders, read-only, token handling, and two-click kill. Whether the
console reports a real failover correctly is the Go integration tests' job, because a mock cannot show that.

## Troubleshooting

| symptom | cause |
| --- | --- |
| `no built frontend found; serving the API only` | run `make web` (or `cd web && npm run build`) |
| `…/bin/scheduler is not an executable binary` | run `make build` |
| `--listen must be a loopback address` | intended; the console only binds loopback |
| `demo mode resets its namespace… must begin with "tq-console"` | intended; pass a `tq-console-*` namespace or none |
| page says `read-only` | open the exact URL the console printed; a new console run has a new token |
| `421` from curl | send `-H 'Host: 127.0.0.1:8088'`, or use the address the console is bound to |
| epochs do not start at 1 after a restart | intended: the epoch counter and fence survive the namespace reset, so an old term can never be re-issued |
| controls fail from `npm run dev` | start the console with `--allow-origin http://127.0.0.1:5173` |

## Known limitations

- The live console watches one Redis and the processes it is told about. It does not discover processes, and in
  monitor mode against Compose it sees the schedulers but not the workers.
- Rates are per poll interval, from counter differences: they are not smoothed and they lag by up to one interval.
- History lives in the console's memory, 10 minutes long, and is lost when it stops. It is not Prometheus.
- Demo processes share one machine, as in every experiment in the report, so the network is never in the failover
  path.
- The research site shows only experiments whose results reproduce. An experiment whose results are not yet
  publishable is listed under `PENDING` in `scripts/export_site_data.py` and appears on the site as pending, with no
  numbers.
