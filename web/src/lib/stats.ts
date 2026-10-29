// Small, explicit statistics. Every function drops nulls (missing measurements)
// rather than treating them as zero, and returns null when nothing is left.

export function present(xs: (number | null | undefined)[]): number[] {
  return xs.filter((x): x is number => typeof x === "number" && Number.isFinite(x));
}

export function mean(xs: (number | null)[]): number | null {
  const v = present(xs);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

/** Linear interpolation between order statistics (pandas' default). */
export function quantile(xs: (number | null)[], q: number): number | null {
  const v = present(xs).sort((a, b) => a - b);
  if (!v.length) return null;
  const pos = (v.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return v[lo]! + (v[hi]! - v[lo]!) * (pos - lo);
}

export const median = (xs: (number | null)[]) => quantile(xs, 0.5);

/** Sample standard deviation (n - 1), as pandas computes it. */
export function stdev(xs: (number | null)[]): number | null {
  const v = present(xs);
  if (v.length < 2) return null;
  const m = v.reduce((a, b) => a + b, 0) / v.length;
  return Math.sqrt(v.reduce((a, b) => a + (b - m) ** 2, 0) / (v.length - 1));
}

export function groupBy<T, K extends string | number>(rows: T[], key: (r: T) => K): Map<K, T[]> {
  const out = new Map<K, T[]>();
  for (const r of rows) {
    const k = key(r);
    const list = out.get(k);
    if (list) list.push(r);
    else out.set(k, [r]);
  }
  return out;
}

/** Relative spread between the largest and smallest of a set of values, in percent. */
export function spreadPct(values: (number | null)[]): number | null {
  const v = present(values);
  if (v.length < 2) return null;
  const lo = Math.min(...v);
  const hi = Math.max(...v);
  return lo > 0 ? ((hi - lo) / lo) * 100 : null;
}

export function unique<T>(xs: T[]): T[] {
  return [...new Set(xs)];
}
