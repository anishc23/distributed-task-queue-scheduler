// One mapping from policy to colour, used by every chart. The colours match
// scripts/plot_results.py, so the static figures in docs/images and the
// interactive charts here can be read against each other.

export const POLICIES = ["fifo", "priority", "edf", "wfq"] as const;
export type Policy = (typeof POLICIES)[number];

export const POLICY_COLOR: Record<string, string> = {
  fifo: "#4C72B0",
  priority: "#DD8452",
  edf: "#55A868",
  wfq: "#C44E52",
  none: "#8a8780",
};

export const POLICY_LABEL: Record<string, string> = {
  fifo: "FIFO",
  priority: "Strict priority",
  edf: "EDF",
  wfq: "WFQ",
  none: "No scheduler",
};

export const WORKLOADS = ["uniform", "bursty", "heavy_tailed", "multi_tenant"] as const;

export const WORKLOAD_LABEL: Record<string, string> = {
  uniform: "Uniform",
  bursty: "Bursty",
  heavy_tailed: "Heavy-tailed",
  multi_tenant: "Multi-tenant skew",
};

export const SYSTEM_COLOR: Record<string, string> = { taskqueue: "#1b1b1a", asynq: "#9a6b1f" };
export const SYSTEM_LABEL: Record<string, string> = { taskqueue: "This queue", asynq: "Asynq v0.24.1" };

export const policyOrder = (xs: string[]) =>
  [...xs].sort((a, b) => order(a) - order(b));
const order = (p: string) => {
  const i = (["none", ...POLICIES] as string[]).indexOf(p);
  return i < 0 ? 99 : i;
};

export const REPO = "https://github.com/anishc23/distributed-task-queue-scheduler";
export const blob = (path: string) => `${REPO}/blob/main/${path}`;
export const tree = (path: string) => `${REPO}/tree/main/${path}`;
