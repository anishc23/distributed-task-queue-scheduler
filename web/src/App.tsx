import { lazy, Suspense, useEffect, useState } from "react";
import { ResearchPage } from "./research/ResearchPage";
import { REPO } from "./lib/vocab";

// The live console is never needed to read the research, so it is split into
// its own chunk and only fetched when someone opens it.
const LivePage = lazy(() => import("./live/LivePage"));

type Mode = "research" | "live";

/**
 * Routing is the hash, so the site works from any static host (including a
 * GitHub Pages project path) with no server rewrites. "#/live" is the live
 * console; any other hash is a section anchor on the research page.
 */
function modeFromHash(): Mode {
  return window.location.hash.startsWith("#/live") ? "live" : "research";
}

export function App() {
  const [mode, setMode] = useState<Mode>(modeFromHash);
  useEffect(() => {
    const on = () => setMode(modeFromHash());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  useEffect(() => {
    document.title = mode === "live" ? "Live console · distributed-task-queue" : "distributed-task-queue";
  }, [mode]);

  return (
    <>
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <header className="topbar">
        <div className="topbar-inner">
          <a className="wordmark" href="#overview" onClick={() => mode === "live" && window.scrollTo(0, 0)}>
            distributed-task-queue
          </a>
          <nav className="modes" aria-label="Mode">
            <a href="#overview" aria-current={mode === "research" ? "page" : undefined}>
              Research
            </a>
            <a href="#/live" aria-current={mode === "live" ? "page" : undefined}>
              Live system
            </a>
          </nav>
          <div className="topbar-links">
            <a href={`${import.meta.env.BASE_URL}docs/report.pdf`}>Report (PDF)</a>
            <a href={REPO}>Source</a>
          </div>
        </div>
      </header>
      {mode === "research" ? (
        <ResearchPage />
      ) : (
        <Suspense fallback={<p className="state-msg" style={{ padding: 32 }}>Loading the live console…</p>}>
          <LivePage />
        </Suspense>
      )}
      <footer className="site">
        Every number on the research page is recomputed from the raw benchmark CSVs by{" "}
        <code>scripts/export_site_data.py</code>, which refuses to export if any disagrees with the report. MIT licence.{" "}
        <a href={REPO}>{REPO.replace("https://", "")}</a>
      </footer>
    </>
  );
}
