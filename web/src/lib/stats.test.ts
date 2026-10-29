import { describe, expect, it } from "vitest";
import { groupBy, mean, median, present, quantile, spreadPct, stdev } from "./stats";

describe("stats", () => {
  it("drops missing values instead of treating them as zero", () => {
    expect(present([1, null, 3, undefined, Number.NaN])).toEqual([1, 3]);
    expect(mean([2, null, 4])).toBe(3);
    expect(mean([null, null])).toBeNull();
  });

  it("quantile interpolates linearly between order statistics, like pandas", () => {
    expect(quantile([1, 2, 3, 4], 0.5)).toBe(2.5);
    expect(quantile([10, 20, 30, 40, 50], 0.95)).toBeCloseTo(48);
    expect(median([5, 1, 3])).toBe(3);
    expect(quantile([], 0.5)).toBeNull();
  });

  it("stdev is the sample standard deviation", () => {
    expect(stdev([2, 4, 4, 4, 5, 5, 7, 9])).toBeCloseTo(2.138, 3);
    expect(stdev([1])).toBeNull();
  });

  it("spreadPct is (max - min) / min", () => {
    expect(spreadPct([100, 150, 193.1])).toBeCloseTo(93.1);
    expect(spreadPct([0, 1])).toBeNull();
    expect(spreadPct([1])).toBeNull();
  });

  it("groupBy preserves insertion order", () => {
    const g = groupBy([{ k: "b" }, { k: "a" }, { k: "b" }], (r) => r.k);
    expect([...g.keys()]).toEqual(["b", "a"]);
    expect(g.get("b")).toHaveLength(2);
  });
});
