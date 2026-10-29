export function Walkthrough({ mode, faults }: { mode: string | null; faults: boolean }) {
  return (
    <details className="panel walkthrough">
      <summary className="panel-h">A ten-minute walkthrough</summary>
      {mode !== "demo" && (
        <p className="small">
          <span className="tag unknown">monitor mode</span> This console is watching a deployment it did not start, so
          the steps that change something are unavailable. Run <code>make console-demo</code> for the full walkthrough.
        </p>
      )}
      {mode === "demo" && !faults && (
        <p className="small">
          <span className="tag unknown">faults off</span> Restart the console with <code>--enable-faults</code> for
          steps 4 to 6.
        </p>
      )}
      <ol className="small">
        <li>
          <strong>Healthy baseline.</strong> Every process ready, exactly one scheduler claiming to lead, the fence
          equal to the leader's epoch.
        </li>
        <li>
          <strong>Submit "Steady".</strong> Dispatch and completion rates rise together; pending stays near zero. Below
          capacity there is no queue, so policy decides nothing (research: <a href="#load">when policy matters</a>).
        </li>
        <li>
          <strong>Submit "Overload".</strong> Pending grows: the backlog now sits in the scheduler's sorted set, where
          the policy decides who waits.
        </li>
        <li>
          <strong>Kill the leader.</strong> Dispatching stops. The dispatch log shows the gap. The standby's own log
          line "became scheduler leader" appears once the old lease expires, and the epoch and fence advance. Compare
          the gap with the 3.5–6 s window the report predicts and measures (<a href="#failover">§4.7</a>). Then restart
          the killed process.
        </li>
        <li>
          <strong>Freeze the leader for 10 s</strong>, longer than its 5 s lease. A standby takes over while the frozen
          process still believes it leads. On resume, look for "fenced out by a newer scheduler epoch" in the events.
          That is the epoch check refusing a live, superseded leader at Redis (<a href="#fencing">§4.8</a>).
        </li>
        <li>
          <strong>Partition the leader for 10 s.</strong> Unlike a freeze, the process keeps running. Its renewals
          fail and a standby takes over, but it keeps reporting itself leader. The claim count shows two. When the link
          heals, its first write is refused. The dispatch log's inversion count stays at zero throughout.
        </li>
      </ol>
      <p className="tiny muted">
        What this does not show: all processes share one machine, so the network is not in the failover path, and
        a demo run is one observation, not a measurement. The measured distributions are on the research page.
      </p>
    </details>
  );
}
