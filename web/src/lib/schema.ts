// Types and runtime validation for the JSON written by
// scripts/export_site_data.py. Nothing reaches a chart without passing through
// validateDataset: a field that is missing, mistyped or NaN is an error the
// page shows, never a value it silently plots as zero.

export const SCHEMA_VERSION = 1;

export interface SourceFile {
  path: string;
  rows: number;
  sha256?: string;
  files?: number;
  sha256_of_set?: string;
}

export interface Dataset<R> {
  id: string;
  title: string;
  report_section: string;
  status: "published";
  schema_version: number;
  command: string;
  conditions: Record<string, unknown>;
  caveats: string[];
  fields: Record<string, string>;
  sources: SourceFile[];
  rows: R[];
}

export interface SweepRow {
  scheduler: string;
  workload: string;
  offered_load: number;
  repetition: number;
  latency_p50_s: number | null;
  latency_p99_s: number | null;
  wait_max_s: number | null;
  deadline_miss_rate: number | null;
  throughput_tasks_per_s: number | null;
  worker_utilization: number | null;
}

export interface MatrixRow {
  run: string;
  scheduler: string;
  workload: string;
  repetition: number;
  latency_p50_s: number | null;
  latency_p95_s: number | null;
  latency_p99_s: number | null;
  wait_max_s: number | null;
  deadline_miss_rate: number | null;
  throughput_tasks_per_s: number | null;
  worker_utilization: number | null;
  jain_fairness_service: number | null;
}

export interface ControlRow {
  scheduler: string;
  workload: string;
  offered_load: number;
  repetition: number;
  throughput_tasks_per_s: number | null;
  deadline_miss_rate: number | null;
  wait_max_s: number | null;
  latency_p99_s: number | null;
}

export interface PriorityRow {
  scheduler: string;
  priority: number;
  latency_p95_s: number;
  tasks: number;
}

export interface TenantRow {
  series: "main" | "sleep" | "cpu";
  scheduler: string;
  tenant: string;
  repetition: number;
  latency_p95_s: number | null;
  service_share: number | null;
}

export interface RealworkRow {
  exec_mode: "sleep" | "cpu";
  scheduler: string;
  repetition: number;
  deadline_miss_rate: number | null;
  latency_p99_s: number | null;
  exec_mean_s: number | null;
}

export interface CeilingRow {
  fenced: boolean;
  phase: "admit" | "dispatch" | "engine";
  policy: string;
  batch: number;
  rate_tasks_per_s: number;
}

export interface FailoverRow {
  run: string;
  arm: string;
  rep: number;
  replicas: number;
  lease_ttl_ms: number;
  lease_renew_ms: number;
  outage_ms: number | null;
  censored: boolean;
  max_gap_ms: number | null;
  tasks: number;
  completed: number;
  unfinished: number;
  duplicate_completions: number;
  epoch_inversions: number;
  pause_ms: number | null;
  fenced_operations: number | null;
  leaders_after_resume: number | null;
}

export interface StorefaultRow {
  arm: string;
  rep: number;
  failover_ms: number | null;
  epoch_reused: boolean;
  submitted: number;
  completed: number;
  tasks_lost: number;
  ingress_lost_entries: number;
  epoch_inversions: number;
}

export interface BaselineRow {
  system: "taskqueue" | "asynq";
  rep: number;
  exec_ms: number;
  tasks: number;
  completed: number;
  concurrency: number;
  throughput_per_s: number | null;
  latency_p99_ms: number | null;
}

export interface PartitionRow {
  run: "partition" | "partition-sweep";
  arm: string;
  rep: number;
  sweep_multiple: number | null;
  lease_ttl_ms: number;
  lease_renew_ms: number;
  partition_ms: number;
  cut_count: number;
  outage_ms: number | null;
  censored: boolean;
  max_gap_ms: number | null;
  failover: boolean;
  unexpected_failover: boolean;
  leader_epochs: number;
  fenced: boolean;
  leaders_after_heal: number | null;
  epoch_inversions: number;
  tasks: number;
  completed: number;
  unfinished: number;
  duplicate_completions: number;
  suspect: boolean;
  host_suspended_ms: number | null;
}

export interface Claim {
  section: string;
  claim: string;
  printed: number;
  computed: number;
  tolerance: number;
  ok: boolean;
}

export interface PendingExperiment {
  id: string;
  title: string;
  harness: string;
  commands: string[];
  status: "pending";
  reason: string;
}

export interface InventoryRow {
  name: string;
  runs: string;
  experiments: number;
  tasks: number;
}

export interface Index {
  schema_version: number;
  generated_by: string;
  datasets: { id: string; title: string; file: string; report_section: string; rows: number; bytes: number }[];
  pending: PendingExperiment[];
  claims: Claim[];
  inventory: { rows: InventoryRow[]; total_experiments: number; total_tasks: number };
}

export interface RowTypes {
  sweep: SweepRow;
  matrix: MatrixRow;
  control: ControlRow;
  priority: PriorityRow;
  tenants: TenantRow;
  realwork: RealworkRow;
  ceiling: CeilingRow;
  failover: FailoverRow;
  storefault: StorefaultRow;
  baseline: BaselineRow;
  partition: PartitionRow;
}
export type DatasetId = keyof RowTypes;

// "number?" admits null (a measurement that is missing), never NaN or a string.
type Kind = "string" | "number" | "number?" | "integer" | "boolean" | `enum:${string}`;
type Spec = Record<string, Kind>;

const SPECS: { [K in DatasetId]: Spec } = {
  sweep: {
    scheduler: "string", workload: "string", offered_load: "number", repetition: "integer",
    latency_p50_s: "number?", latency_p99_s: "number?", wait_max_s: "number?",
    deadline_miss_rate: "number?", throughput_tasks_per_s: "number?", worker_utilization: "number?",
  },
  matrix: {
    run: "string", scheduler: "string", workload: "string", repetition: "integer",
    latency_p50_s: "number?", latency_p95_s: "number?", latency_p99_s: "number?",
    wait_max_s: "number?", deadline_miss_rate: "number?", throughput_tasks_per_s: "number?",
    worker_utilization: "number?", jain_fairness_service: "number?",
  },
  control: {
    scheduler: "string", workload: "string", offered_load: "number", repetition: "integer",
    throughput_tasks_per_s: "number?", deadline_miss_rate: "number?", wait_max_s: "number?",
    latency_p99_s: "number?",
  },
  priority: { scheduler: "string", priority: "integer", latency_p95_s: "number", tasks: "integer" },
  tenants: {
    series: "enum:main|sleep|cpu", scheduler: "string", tenant: "string", repetition: "integer",
    latency_p95_s: "number?", service_share: "number?",
  },
  realwork: {
    exec_mode: "enum:sleep|cpu", scheduler: "string", repetition: "integer",
    deadline_miss_rate: "number?", latency_p99_s: "number?", exec_mean_s: "number?",
  },
  ceiling: {
    fenced: "boolean", phase: "enum:admit|dispatch|engine", policy: "string", batch: "integer",
    rate_tasks_per_s: "number",
  },
  failover: {
    run: "string", arm: "string", rep: "integer", replicas: "integer", lease_ttl_ms: "integer",
    lease_renew_ms: "integer", outage_ms: "number?", censored: "boolean", max_gap_ms: "number?",
    tasks: "integer", completed: "integer", unfinished: "integer",
    duplicate_completions: "integer", epoch_inversions: "integer", pause_ms: "number?",
    fenced_operations: "number?", leaders_after_resume: "number?",
  },
  storefault: {
    arm: "string", rep: "integer", failover_ms: "number?", epoch_reused: "boolean",
    submitted: "integer", completed: "integer", tasks_lost: "integer",
    ingress_lost_entries: "integer", epoch_inversions: "integer",
  },
  baseline: {
    system: "enum:taskqueue|asynq", rep: "integer", exec_ms: "integer", tasks: "integer",
    completed: "integer", concurrency: "integer", throughput_per_s: "number?", latency_p99_ms: "number?",
  },
  partition: {
    run: "enum:partition|partition-sweep", arm: "string", rep: "integer", sweep_multiple: "number?",
    lease_ttl_ms: "integer", lease_renew_ms: "integer", partition_ms: "number", cut_count: "integer",
    outage_ms: "number?", censored: "boolean", max_gap_ms: "number?", failover: "boolean",
    unexpected_failover: "boolean", leader_epochs: "integer", fenced: "boolean",
    leaders_after_heal: "number?", epoch_inversions: "integer", tasks: "integer", completed: "integer",
    unfinished: "integer", duplicate_completions: "integer", suspect: "boolean", host_suspended_ms: "number?",
  },
};

export class ValidationError extends Error {
  constructor(public readonly problems: string[]) {
    super(`${problems.length} problem(s): ${problems.slice(0, 3).join("; ")}`);
    this.name = "ValidationError";
  }
}

function check(value: unknown, kind: Kind): boolean {
  switch (kind) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "number?":
      return value === null || (typeof value === "number" && Number.isFinite(value));
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    case "boolean":
      return typeof value === "boolean";
    default:
      return typeof value === "string" && kind.slice(5).split("|").includes(value);
  }
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

export function validateDataset<K extends DatasetId>(id: K, raw: unknown): Dataset<RowTypes[K]> {
  const problems: string[] = [];
  if (!isObject(raw)) throw new ValidationError(["dataset is not an object"]);
  if (raw.schema_version !== SCHEMA_VERSION) {
    problems.push(`schema_version ${String(raw.schema_version)}, expected ${SCHEMA_VERSION}`);
  }
  if (raw.id !== id) problems.push(`id ${String(raw.id)}, expected ${id}`);
  if (raw.status !== "published") problems.push(`status ${String(raw.status)} is not publishable`);
  for (const key of ["title", "report_section", "command"]) {
    if (typeof raw[key] !== "string") problems.push(`${key} missing`);
  }
  if (!Array.isArray(raw.sources) || raw.sources.length === 0) {
    problems.push("no sources: a dataset without provenance is not publishable");
  } else {
    raw.sources.forEach((s, i) => {
      if (!isObject(s) || typeof s.path !== "string" || typeof s.rows !== "number") {
        problems.push(`sources[${i}] malformed`);
      } else if (typeof s.sha256 !== "string" && typeof s.sha256_of_set !== "string") {
        problems.push(`sources[${i}] has no digest`);
      }
    });
  }
  if (!Array.isArray(raw.caveats)) problems.push("caveats missing");
  if (!Array.isArray(raw.rows) || raw.rows.length === 0) {
    problems.push("rows missing or empty");
  } else {
    const spec = SPECS[id];
    raw.rows.forEach((row, i) => {
      if (problems.length > 20) return;
      if (!isObject(row)) {
        problems.push(`rows[${i}] is not an object`);
        return;
      }
      for (const [field, kind] of Object.entries(spec)) {
        if (!(field in row)) problems.push(`rows[${i}].${field} missing`);
        else if (!check(row[field], kind)) problems.push(`rows[${i}].${field}=${JSON.stringify(row[field])} is not ${kind}`);
      }
    });
  }
  if (problems.length) throw new ValidationError(problems);
  return raw as unknown as Dataset<RowTypes[K]>;
}

export function validateIndex(raw: unknown): Index {
  const problems: string[] = [];
  if (!isObject(raw)) throw new ValidationError(["index is not an object"]);
  if (raw.schema_version !== SCHEMA_VERSION) problems.push("index schema_version mismatch");
  if (!Array.isArray(raw.claims)) problems.push("claims missing");
  if (!Array.isArray(raw.pending)) problems.push("pending missing");
  if (!Array.isArray(raw.datasets)) problems.push("datasets missing");
  const inv = raw.inventory;
  if (!isObject(inv) || !Array.isArray(inv.rows)) problems.push("inventory missing");
  if (Array.isArray(raw.claims)) {
    raw.claims.forEach((c, i) => {
      if (!isObject(c) || typeof c.ok !== "boolean" || typeof c.printed !== "number" || typeof c.computed !== "number") {
        problems.push(`claims[${i}] malformed`);
      }
    });
  }
  if (problems.length) throw new ValidationError(problems);
  return raw as unknown as Index;
}
