import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { type DatasetId, ValidationError, validateDataset, validateIndex } from "./schema";

const DATA = join(__dirname, "..", "..", "public", "data");
const load = (f: string) => JSON.parse(readFileSync(join(DATA, f), "utf8")) as Record<string, unknown>;
const IDS: DatasetId[] = ["sweep", "matrix", "control", "priority", "tenants", "realwork", "ceiling", "failover", "storefault", "baseline", "partition"];

describe("the committed datasets", () => {
  it.each(IDS)("%s validates against its schema", (id) => {
    const ds = validateDataset(id, load(`${id}.json`));
    expect(ds.rows.length).toBeGreaterThan(0);
    expect(ds.sources.length).toBeGreaterThan(0);
  });

  it("covers every file the exporter wrote, and nothing else", () => {
    const files = readdirSync(DATA).filter((f) => f.endsWith(".json") && f !== "index.json");
    expect(files.map((f) => f.replace(".json", "")).sort()).toEqual([...IDS].sort());
  });

  it("index: every report claim reproduces", () => {
    const ix = validateIndex(load("index.json"));
    expect(ix.claims.length).toBeGreaterThan(50);
    expect(ix.claims.filter((c) => !c.ok)).toEqual([]);
  });

  it("index: inventory totals are the sum of their rows", () => {
    const ix = validateIndex(load("index.json"));
    expect(ix.inventory.total_experiments).toBe(ix.inventory.rows.reduce((s, r) => s + r.experiments, 0));
    expect(ix.inventory.total_tasks).toBe(ix.inventory.rows.reduce((s, r) => s + r.tasks, 0));
  });

  it("pending experiments carry no numbers and no published dataset", () => {
    const ix = validateIndex(load("index.json"));
    for (const p of ix.pending) {
      expect(p.status).toBe("pending");
      expect(IDS).not.toContain(p.id as DatasetId);
      expect(Object.keys(p).sort()).toEqual(["commands", "harness", "id", "reason", "status", "title"]);
    }
  });

  it("failover: a censored trial never carries an outage value", () => {
    const ds = validateDataset("failover", load("failover.json"));
    const censored = ds.rows.filter((r) => r.censored);
    expect(censored.length).toBeGreaterThan(0);
    for (const r of censored) expect(r.outage_ms).toBeNull();
    for (const r of ds.rows.filter((x) => !x.censored)) expect(r.outage_ms).not.toBeNull();
  });

  it("failover: the single-writer invariant held in every published trial", () => {
    const ds = validateDataset("failover", load("failover.json"));
    expect(ds.rows.reduce((s, r) => s + r.epoch_inversions, 0)).toBe(0);
  });

  it("partition: the single-writer invariant held, and suspect trials stay flagged", () => {
    const ds = validateDataset("partition", load("partition.json"));
    expect(ds.rows.reduce((s, r) => s + r.epoch_inversions, 0)).toBe(0);
    for (const r of ds.rows.filter((x) => x.partition_ms > 0 && !x.suspect)) expect(r.leaders_after_heal).toBe(1);
    for (const r of ds.rows.filter((x) => x.partition_ms === 0)) expect(r.outage_ms).toBeNull();
    expect(ds.caveats.join(" ")).toMatch(/suspect/);
  });

  it("matrix: the bursty load caveat travels with the data", () => {
    const ds = validateDataset("matrix", load("matrix.json"));
    expect(ds.caveats.join(" ")).toMatch(/bursty[\s\S]*1\.57x/);
  });
});

describe("validateDataset rejects what must not be plotted", () => {
  const good = () => load("baseline.json");

  it("a missing measurement field", () => {
    const d = good();
    delete (d.rows as Record<string, unknown>[])[0]!.throughput_per_s;
    expect(() => validateDataset("baseline", d)).toThrow(/throughput_per_s missing/);
  });

  it("a number serialised as a string", () => {
    const d = good();
    (d.rows as Record<string, unknown>[])[0]!.exec_ms = "5";
    expect(() => validateDataset("baseline", d)).toThrow(ValidationError);
  });

  it("null in a field that is not allowed to be missing", () => {
    const d = good();
    (d.rows as Record<string, unknown>[])[0]!.rep = null;
    expect(() => validateDataset("baseline", d)).toThrow(/rep=null/);
  });

  it("null where a measurement may be missing is accepted, not coerced", () => {
    const d = good();
    (d.rows as Record<string, unknown>[])[0]!.latency_p99_ms = null;
    const ds = validateDataset("baseline", d);
    expect(ds.rows[0]!.latency_p99_ms).toBeNull();
  });

  it("an unknown enum value", () => {
    const d = good();
    (d.rows as Record<string, unknown>[])[0]!.system = "celery";
    expect(() => validateDataset("baseline", d)).toThrow(/system/);
  });

  it("a dataset with no provenance", () => {
    const d = good();
    d.sources = [];
    expect(() => validateDataset("baseline", d)).toThrow(/provenance/);
  });

  it("a source without a digest", () => {
    const d = good();
    d.sources = [{ path: "results/x.csv", rows: 3 }];
    expect(() => validateDataset("baseline", d)).toThrow(/digest/);
  });

  it("a dataset that is not marked publishable", () => {
    const d = good();
    d.status = "pending";
    expect(() => validateDataset("baseline", d)).toThrow(/not publishable/);
  });

  it("a schema version it does not understand", () => {
    const d = good();
    d.schema_version = 2;
    expect(() => validateDataset("baseline", d)).toThrow(/schema_version/);
  });

  it("the wrong dataset under a name", () => {
    expect(() => validateDataset("sweep", good())).toThrow(/id baseline, expected sweep/);
  });
});
