const group = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });

export function fmt(x: number | null | undefined, digits = 3): string {
  if (x === null || x === undefined || !Number.isFinite(x)) return "—";
  if (Math.abs(x) >= 1000) return group.format(x);
  return x.toFixed(digits);
}

export const fmtInt = (x: number | null | undefined) =>
  x === null || x === undefined || !Number.isFinite(x) ? "—" : group.format(Math.round(x));

export const fmtPct = (x: number | null | undefined, digits = 1) =>
  x === null || x === undefined || !Number.isFinite(x) ? "—" : `${x.toFixed(digits)}%`;

export const fmtMs = (x: number | null | undefined) =>
  x === null || x === undefined || !Number.isFinite(x) ? "—" : `${group.format(Math.round(x))} ms`;

export const shortHash = (h?: string) => (h ? h.slice(0, 12) : "");
