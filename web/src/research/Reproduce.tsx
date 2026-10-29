import { Block } from "./ResearchPage";
import { REPO, blob, tree } from "../lib/vocab";

export function Reproduce() {
  return (
    <Block id="reproduce" title="Reproduce and read further" refs="report Appendix A">
      <p className="small">
        Needs Go, a local Redis, Python 3 for the charts, and an otherwise idle machine on mains power for anything
        that measures wall-clock gaps. The commands below are the ones the report's numbers came from.
      </p>
      <pre className="small">
        <code>{`make redis-up
make bench-full REPETITIONS=25 RUN=main            # §4.1
make sweep RUN=sweep-loads REPETITIONS=3           # §4.4
go run ./cmd/benchmark --schedulers none,fifo,priority,edf,wfq \\
  --loads 0.75,1.0,1.25,1.5 --repetitions 3 --run-id control-arm   # §4.6
go run ./cmd/schedbench --tasks 20000 --epoch 0 --csv results/ceiling-unfenced/ceiling.csv
go run ./cmd/schedbench --tasks 20000 --epoch 1 --csv results/ceiling-fenced/ceiling.csv
make failover FAILOVER_REPETITIONS=15 RUN=failover  # §4.7, §4.8
make storefault STOREFAULT_REPETITIONS=10 RUN=storefault   # §4.9, needs redis-sentinel
make baseline BASELINE_REPETITIONS=5 RUN=baseline  # §4.10, separate Go module
go run ./cmd/benchmark --config experiments/realwork.yaml \\
  --exec-mode cpu --repetitions 25 --run-id realwork # §5.3`}</code>
      </pre>
      <h3>Rebuilding this site from those results</h3>
      <pre className="small">
        <code>{`python3 scripts/export_site_data.py --check   # recompute every quoted number
python3 scripts/export_site_data.py           # write web/public/data/*.json
cd web && npm ci && npm run build             # static site in web/dist`}</code>
      </pre>
      <p className="small">
        The exported JSON is committed, so the site builds without the 1.5 GB <code>results/</code> directory. Each
        file names the CSVs it came from with their SHA-256, so a reader can check the JSON against a fresh run.
      </p>
      <h3>Documents</h3>
      <ul className="small">
        <li>
          <a href={`${import.meta.env.BASE_URL}docs/report.pdf`}>Report (PDF)</a> ·{" "}
          <a href={blob("docs/report.md")}>Markdown source</a>
        </li>
        <li>
          <a href={blob("docs/architecture.md")}>Architecture notes</a> — design decisions and failure modes
        </li>
        <li>
          <a href={blob("docs/experiments.md")}>Experiment guide</a> — workloads, configurations, how to read each
          metric
        </li>
        <li>
          <a href={blob("docs/metrics.md")}>Metrics reference</a> — every Prometheus series and the queries worth
          running
        </li>
        <li>
          <a href={blob("results/README.md")}>Result schemas</a> — every CSV column, including the censoring rules
        </li>
        <li>
          <a href={tree("docs/images")}>Static figures</a> — the matplotlib charts embedded in the README
        </li>
        <li>
          <a href={REPO}>Repository</a> · MIT licence
        </li>
      </ul>
    </Block>
  );
}
