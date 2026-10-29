import * as Plot from "@observablehq/plot";

export { Plot };

/** Shared options so every chart has the same type, margins and colours. */
export function frame(width: number, height = 320): Plot.PlotOptions {
  return {
    width,
    height,
    marginLeft: 56,
    marginRight: 20,
    marginTop: 18,
    marginBottom: 40,
    style: {
      fontFamily: "var(--font-mono)",
      fontSize: "11px",
      background: "transparent",
      color: "var(--ink-2)",
      overflow: "visible",
    },
  };
}

/** Plot's tick/label colour follows currentColor; rules and grid need it explicitly. */
export const GRID = { stroke: "var(--rule)", strokeOpacity: 1 } as const;
