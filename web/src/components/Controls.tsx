import type { ReactNode } from "react";

export interface Option<T extends string> {
  value: T;
  label: string;
  /** Shown beside the label, e.g. a warning marker; must also be in `hint`. */
  flag?: string;
  hint?: string;
}

/**
 * A row of mutually exclusive buttons. Only values that exist in the data are
 * ever passed as options, so a comparison the data cannot support cannot be
 * selected.
 */
export function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: Option<T>[];
  onChange: (v: T) => void;
}) {
  return (
    <div className="control" role="group" aria-label={label}>
      <span className="control-label" aria-hidden="true">
        {label}
      </span>
      <div className="seg">
        {options.map((o) => (
          <button
            key={o.value}
            type="button"
            aria-pressed={o.value === value}
            title={o.hint}
            onClick={() => onChange(o.value)}
          >
            {o.label}
            {o.flag && (
              <span className="flag" aria-label={o.hint}>
                {o.flag}
              </span>
            )}
          </button>
        ))}
      </div>
    </div>
  );
}

export function Legend({ items }: { items: { label: string; color: string; shape?: "line" | "dot" }[] }) {
  return (
    <div className="legend" aria-hidden="true">
      {items.map((i) => (
        <span key={i.label}>
          <i className={`swatch${i.shape === "dot" ? " dot" : ""}`} style={{ background: i.color }} />
          {i.label}
        </span>
      ))}
    </div>
  );
}

export function Note({
  kind = "plain",
  title,
  children,
}: {
  kind?: "plain" | "caveat" | "negative" | "pending";
  title?: string;
  children: ReactNode;
}) {
  return (
    <div className={`note ${kind === "plain" ? "" : kind}`} role={kind === "caveat" || kind === "pending" ? "note" : undefined}>
      {title && <div className="note-title">{title}</div>}
      {children}
    </div>
  );
}

export function Table({
  head,
  rows,
  numeric = [],
  caption,
}: {
  head: string[];
  rows: ReactNode[][];
  numeric?: number[];
  caption?: string;
}) {
  return (
    <table className="t">
      {caption && <caption className="visually-hidden">{caption}</caption>}
      <thead>
        <tr>
          {head.map((h, i) => (
            <th key={i} scope="col" className={numeric.includes(i) ? "r" : undefined}>
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>
            {r.map((c, j) => (
              <td key={j} className={numeric.includes(j) ? "r num" : undefined}>
                {c}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
