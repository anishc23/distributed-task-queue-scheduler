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
}: {
  build: (width: number) => Element;
  deps: unknown[];
  label: string;
  height?: number;
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
      node = build(width);
    } catch (err) {
      const p = document.createElement("p");
      p.className = "state-msg error";
      p.textContent = `This chart could not be drawn: ${err instanceof Error ? err.message : String(err)}`;
      node = p;
    }
    node.setAttribute("role", "img");
    node.setAttribute("aria-label", label);
    el.replaceChildren(node);
    return () => {
      el.replaceChildren();
    };
    // build is recreated each render; deps carries what it actually reads.
  }, [width, label, ...deps]);

  return <div ref={ref} style={{ minHeight: height }} />;
}
