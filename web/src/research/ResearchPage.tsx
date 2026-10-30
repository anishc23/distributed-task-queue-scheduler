import { lazy, Suspense, useEffect, useState } from "react";
import { Overview } from "./Overview";
import { Architecture } from "./Architecture";
import { Method } from "./Method";

const ResultSections = lazy(() => import("./ResultSections"));

const SECTIONS: { group?: string; id: string; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "architecture", label: "Architecture" },
  { id: "method", label: "Method" },
  { group: "Scheduling", id: "load", label: "When policy matters" },
  { id: "matrix", label: "Policy by workload" },
  { id: "isolation", label: "Starvation and isolation" },
  { id: "cost", label: "What centralising costs" },
  { group: "Failure", id: "failover", label: "Scheduler failover" },
  { id: "fencing", label: "Why the fence exists" },
  { id: "store", label: "When Redis fails over" },
  { id: "partition", label: "Cut off from Redis" },
  { group: "Validity", id: "realwork", label: "Sleep is not neutral" },
  { id: "corrections", label: "Corrections" },
  { id: "limitations", label: "Limitations and open work" },
  { id: "reproduce", label: "Reproduce" },
];

/** The section whose top has most recently scrolled past the reading line. */
function useActiveSection(): string {
  const [active, setActive] = useState("overview");
  useEffect(() => {
    let frame = 0;
    const update = () => {
      frame = 0;
      const line = 140;
      let current = SECTIONS[0]!.id;
      for (const s of SECTIONS) {
        const el = document.getElementById(s.id);
        if (el && el.getBoundingClientRect().top <= line) current = s.id;
      }
      setActive(current);
    };
    const onScroll = () => {
      if (!frame) frame = requestAnimationFrame(update);
    };
    update();
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll);
    return () => {
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
      if (frame) cancelAnimationFrame(frame);
    };
  }, []);
  return active;
}

export function ResearchPage() {
  const active = useActiveSection();

  // A hash that names a section scrolls to it once the section exists; the
  // browser does this itself only if the element is present at load time.
  // Charts load asynchronously and push later sections down, so the target is
  // kept pinned while the layout settles, until the reader scrolls themselves.
  useEffect(() => {
    const id = window.location.hash.slice(1);
    const main = document.getElementById("main");
    if (!id || id.startsWith("/") || !main) return;
    // Otherwise the browser restores the previous offset on reload, after us.
    if ("scrollRestoration" in history) history.scrollRestoration = "manual";
    const pin = () => document.getElementById(id)?.scrollIntoView();
    pin();
    const ro = new ResizeObserver(pin);
    ro.observe(main);
    const release = () => ro.disconnect();
    const timer = window.setTimeout(release, 3000);
    const events = ["wheel", "touchstart", "keydown", "mousedown"] as const;
    events.forEach((e) => window.addEventListener(e, release, { once: true, passive: true }));
    return () => {
      release();
      window.clearTimeout(timer);
      events.forEach((e) => window.removeEventListener(e, release));
    };
  }, []);

  return (
    <div className="layout">
      <nav className="toc" aria-label="Sections">
        <ol>
          {SECTIONS.map((s) => (
            <li key={s.id}>
              {s.group && <div className="toc-group">{s.group}</div>}
              <a href={`#${s.id}`} aria-current={active === s.id ? "true" : undefined}>
                {s.label}
              </a>
            </li>
          ))}
        </ol>
      </nav>
      <main id="main" tabIndex={-1}>
        <Overview />
        <Architecture />
        <Method />
        <Suspense fallback={<p className="state-msg" aria-live="polite">Loading the results…</p>}>
          <ResultSections />
        </Suspense>
      </main>
    </div>
  );
}

export function Block({ id, title, refs, children }: { id: string; title: string; refs?: string; children: React.ReactNode }) {
  return (
    <section className="block" id={id} aria-labelledby={`${id}-h`}>
      <div className="block-head">
        <h2 id={`${id}-h`}>{title}</h2>
        {refs && <span className="ref">{refs}</span>}
      </div>
      {children}
    </section>
  );
}
