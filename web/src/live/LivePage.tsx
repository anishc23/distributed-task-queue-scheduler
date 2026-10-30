import { useState } from "react";
import { useConsole } from "./useConsole";
import { captureToken, forgetToken, type Snapshot } from "./api";
import { ConnectionBar, Kpis, Leadership, Processes, Events, DeadLetters, Tenants } from "./Panels";
import { HistoryCharts, DispatchLog } from "./LiveCharts";
import { ControlPanel } from "./ControlPanel";
import { Walkthrough } from "./Walkthrough";

export default function LivePage() {
  const live = useConsole();
  const [token, setToken] = useState<string | null>(() => captureToken());
  const { connection, snapshot } = live;
  const old = connection.kind === "disconnected" || connection.kind === "stale";

  return (
    <div className="live">
      <main id="main" tabIndex={-1} className="live-main">
        <header className="live-head">
          <div>
            <div className="kicker">Live system · current telemetry</div>
            <h1>Operations console</h1>
          </div>
          <ConnectionBar live={live} />
        </header>
        <p className="live-scope small muted">
          Everything on this page is read now from a queue running on this machine: Redis directly, and each process's{" "}
          <code>/healthz</code>, <code>/readyz</code> and <code>/metrics</code>. None of it is a published result. The
          measured results are on the <a href="#overview">research page</a>.
        </p>

        {connection.kind === "unavailable" || (connection.kind === "disconnected" && !snapshot) ? (
          <NoConsole reason={connection.reason} />
        ) : connection.kind === "connecting" ? (
          <p className="state-msg" aria-live="polite">
            Connecting to the console…
          </p>
        ) : (
          <div className="live-grid" data-old={old || undefined}>
            {old && (
              <div className="note caveat live-old" role="status">
                <div className="note-title">Not current</div>
                <p>
                  {connection.kind === "stale"
                    ? "The console is answering but has not completed a poll recently, so these values are old. Its own Redis or scrape requests may be hanging."
                    : "The console is not answering. The values below are the last ones received and may no longer be true."}
                </p>
              </div>
            )}
            <Kpis snap={snapshot!} />
            <div className="live-row">
              <Leadership snap={snapshot!} />
              <Processes snap={snapshot!} now={live.now} />
            </div>
            <HistoryCharts samples={live.history} />
            <DispatchLog
              window={live.dispatches}
              waiting={snapshot?.queue ? snapshot.queue.pending + snapshot.queue.not_admitted : null}
              samples={live.history}
            />
            <div className="live-row">
              <ControlPanel
                live={live}
                token={token}
                onForget={() => {
                  forgetToken();
                  setToken(null);
                }}
              />
              <Events events={live.events} />
            </div>
            <div className="live-row">
              <Tenants window={live.tenants} policy={snapshot?.policy} />
              <DeadLetters entries={live.dlq} error={live.dlqError} />
            </div>
            <Walkthrough mode={live.info?.mode ?? null} faults={live.controls?.faults_enabled ?? false} />
          </div>
        )}
      </main>
    </div>
  );
}

function NoConsole({ reason }: { reason: string }) {
  return (
    <section className="note pending" aria-labelledby="noconsole-h">
      <h2 id="noconsole-h" className="note-title">
        No live console is connected
      </h2>
      <p>
        The live view talks to a console process running on your own machine, on a loopback address. A page served
        from anywhere else cannot reach one, by design. Fault injection must never be reachable from the internet. To
        see it, run the demo locally:
      </p>
      <pre className="small" tabIndex={0}>
        <code>{`make redis-up          # or any local Redis on :6379
make console-demo      # builds everything, starts 2 schedulers + 2 workers
# then open the URL it prints: http://127.0.0.1:8088/#/live?token=…`}</code>
      </pre>
      <p className="small muted">Last error: {reason}</p>
    </section>
  );
}

export type { Snapshot };
