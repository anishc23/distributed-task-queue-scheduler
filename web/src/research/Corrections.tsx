import { Block } from "./ResearchPage";
import { useIndex } from "../lib/data";
import { Loaded } from "../components/Loaded";
import { Table } from "../components/Controls";
import { blob } from "../lib/vocab";

export function Corrections() {
  const index = useIndex();
  return (
    <Block id="corrections" title="Corrections" refs="report §5.1">
      <p>
        The study is reported with its mistakes rather than without them. Each of these was a claim made in an
        earlier revision, or a defect in the system, found by more data rather than by review.
      </p>
      <ol>
        <li>
          <strong>The bursty load confound.</strong> Bursty experiments ran at 1.57× capacity while the others sat at
          1.25×, because that workload ignores its nominal arrival rate. This is why bursty once showed a 0.94 miss
          rate. The load sweep corrects it by deriving each workload's rate from measured capacity.
        </li>
        <li>
          <strong>EDF under overload.</strong> The claim that EDF "degrades sharply under overload" came from theory
          and was written three times before anything was measured. The sweep contradicted it, but the correction then
          overreached in the other direction. See the last item.
        </li>
        <li>
          <strong>EDF's real-work degradation.</strong> First reported as 27× from three repetitions, on the one
          workload already known to need twenty-five. At n = 25 it is 8.4×.
        </li>
        <li>
          <strong>A lost task, found by the failover experiment.</strong> Every killed-leader trial finished exactly
          one task short. An ingress entry the dead leader had read but not acknowledged belonged to a consumer that
          would never return. It was fixed with an <code>XAUTOCLAIM</code> pass over the ingress group, and the
          regression test fails without it.
        </li>
        <li>
          <strong>A deadlock, found by the same experiment.</strong> A leadership term that ended by itself, which is
          what a fenced leader does, deadlocked the lease's campaign loop. A test of three consecutive self-ending
          terms reproduces it.
        </li>
        <li>
          <strong>Four errors found by building this site.</strong> The export script recomputes every number the
          report quotes and refuses to publish on any disagreement. On its first run it found that: (a) the main
          matrix's bursty rows were still at about 1.57× load, though described as 1.25×; (b) EDF was described as the
          best policy "at every load", beside a table showing strict priority ahead at 2× and 3×; (c) "122×" and
          "129×" had been divided from rounded values, and are 118× and 117×; (d) the store-failure trials used
          2,000 tasks, not 3,000, so the study's task total is 5.44 M, not 5.47 M. None of these changes a
          conclusion's direction. The report now carries all four.
        </li>
      </ol>

      <h3 style={{ marginTop: 12 }}>The reproduction check</h3>
      <Loaded load={index} what="the claims check">
        {(ix) => {
          const ok = ix.claims.filter((c) => c.ok).length;
          return (
            <>
              <p className="small">
                {ok} of {ix.claims.length} quoted numbers reproduce from the raw CSVs, at the precision the report
                prints them. The script is <a href={blob("scripts/export_site_data.py")}>scripts/export_site_data.py</a>
                . It uses the Python standard library only and never writes output when a check fails.
              </p>
              <details className="data-table">
                <summary>All {ix.claims.length} checks</summary>
                <div className="table-scroll">
                  <Table
                    caption="Every report claim recomputed"
                    head={["§", "claim", "report", "recomputed", "tolerance", ""]}
                    numeric={[2, 3, 4]}
                    rows={ix.claims.map((c) => [
                      c.section,
                      c.claim,
                      String(c.printed),
                      String(c.computed),
                      `±${c.tolerance}`,
                      c.ok ? <span className="tag ok">match</span> : <span className="tag bad">differs</span>,
                    ])}
                  />
                </div>
              </details>
            </>
          );
        }}
      </Loaded>
    </Block>
  );
}
