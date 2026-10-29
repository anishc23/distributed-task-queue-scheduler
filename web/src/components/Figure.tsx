import type { ReactNode } from "react";
import type { Dataset, SourceFile } from "../lib/schema";
import { shortHash } from "../lib/format";
import { blob } from "../lib/vocab";

/**
 * A chart with everything needed to check it: what it shows, the conditions it
 * was measured under, the exact files it came from, and the numbers as a table.
 */
export function Figure({
  id,
  title,
  controls,
  legend,
  children,
  caption,
  dataset,
  table,
  extraSource,
}: {
  id?: string;
  title: string;
  controls?: ReactNode;
  legend?: ReactNode;
  children: ReactNode;
  caption: ReactNode;
  dataset?: Dataset<unknown>;
  table?: ReactNode;
  extraSource?: ReactNode;
}) {
  return (
    <figure className="fig" id={id} aria-labelledby={id ? `${id}-title` : undefined}>
      <div className="fig-head">
        <h3 id={id ? `${id}-title` : undefined}>{title}</h3>
        {dataset && <span className="provenance">report §{dataset.report_section}</span>}
      </div>
      {controls && <div className="fig-controls">{controls}</div>}
      {legend}
      <div className="fig-body">{children}</div>
      <figcaption>
        {caption}
        {dataset && <Provenance dataset={dataset} extra={extraSource} />}
        {table && (
          <details className="data-table">
            <summary>Numbers behind this chart</summary>
            <div className="table-scroll">{table}</div>
          </details>
        )}
      </figcaption>
    </figure>
  );
}

export function Provenance({ dataset, extra }: { dataset: Dataset<unknown>; extra?: ReactNode }) {
  return (
    <details className="provenance">
      <summary>
        Source: {dataset.sources.map((s) => s.path).join(", ")} · {dataset.rows.length} rows
      </summary>
      <dl>
        <dt>dataset</dt>
        <dd>
          <a href={`${import.meta.env.BASE_URL}data/${dataset.id}.json`}>{dataset.id}.json</a> — exported by{" "}
          <a href={blob("scripts/export_site_data.py")}>scripts/export_site_data.py</a>
        </dd>
        {dataset.sources.map((s) => (
          <SourceLine key={s.path} s={s} />
        ))}
        <dt>produced by</dt>
        <dd>
          <pre style={{ margin: "2px 0", padding: "6px 8px", fontSize: "0.75rem" }}>
            <code>{dataset.command}</code>
          </pre>
        </dd>
        {extra}
        {dataset.caveats.map((c, i) => (
          <Caveat key={i} text={c} />
        ))}
      </dl>
    </details>
  );
}

function SourceLine({ s }: { s: SourceFile }) {
  return (
    <>
      <dt>file</dt>
      <dd>
        {s.path} · {s.files ? `${s.files} files · ` : ""}
        {s.rows.toLocaleString("en-GB")} rows · sha256 {shortHash(s.sha256 ?? s.sha256_of_set)}
        {s.sha256_of_set ? " (of the set)" : ""}
      </dd>
    </>
  );
}

function Caveat({ text }: { text: string }) {
  return (
    <>
      <dt>caveat</dt>
      <dd>{text}</dd>
    </>
  );
}
