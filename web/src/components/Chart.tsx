import { useEffect, useRef, useState } from "react";

/**
 * Renders an Observable Plot figure at the width of its container and
 * re-renders when that width or `deps` change. `build` receives the width and
 * returns the element Plot produced.
 */
export function Chart({
  build,
  deps,
  label,
  height = 320,
  minWidth = 560,
}: {
  build: (width: number) => Element;
  deps: unknown[];
  label: string;
  height?: number;
  /** Below this the chart keeps its width and scrolls sideways in its frame. */
  minWidth?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => {
      const w = Math.floor(entry?.contentRect.width ?? 0);
      setWidth((prev) => (Math.abs(prev - w) > 4 ? w : prev));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    const el = ref.current;
    if (!el || width === 0) return;
    let node: Element;
    try {
      node = build(Math.max(width, minWidth));
    } catch (err) {
      const p = document.createElement("p");
      p.className = "state-msg error";
      p.textContent = `This chart could not be drawn: ${err instanceof Error ? err.message : String(err)}`;
      node = p;
    }
    // The chart is one image with one label. Plot also labels its inner
    // groups, which is invalid on an element with no role and is noise to a
    // screen reader that has already been told what the image is.
    node.querySelectorAll("[aria-label]").forEach((el) => el.removeAttribute("aria-label"));
    node.setAttribute("role", "img");
    node.setAttribute("aria-label", label);
    el.replaceChildren(node);
    return () => {
      el.replaceChildren();
    };
    // build is recreated each render; deps carries what it actually reads.
  }, [width, label, ...deps]);

  const scrolls = width > 0 && width < minWidth;
  return (
    <>
      <div
        ref={ref}
        style={{ minHeight: height, overflowX: scrolls ? "auto" : undefined }}
        // A region that scrolls must be reachable by keyboard, and say what it is.
        tabIndex={scrolls ? 0 : undefined}
        aria-label={scrolls ? `${label} (scrolls sideways)` : undefined}
        role={scrolls ? "region" : undefined}
      />
      {scrolls && (
        <p className="tiny muted" aria-hidden="true" style={{ margin: "2px 0 0" }}>
          Scroll the chart sideways to see all of it.
        </p>
      )}
    </>
  );
}
