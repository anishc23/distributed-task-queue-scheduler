import type { ReactNode } from "react";
import type { Load } from "../lib/data";

/** Renders children only once data has loaded and validated; says why otherwise. */
export function Loaded<T>({ load, what, children }: { load: Load<T>; what: string; children: (v: T) => ReactNode }) {
  if (load.state === "loading") {
    return (
      <p className="state-msg" aria-live="polite">
        Loading {what}…
      </p>
    );
  }
  if (load.state === "error") {
    return (
      <p className="state-msg error" role="alert">
        {what} could not be shown: {load.message}. Nothing is plotted in its place.
      </p>
    );
  }
  return <>{children(load.value)}</>;
}
