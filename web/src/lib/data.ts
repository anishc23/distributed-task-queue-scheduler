import { useEffect, useState } from "react";
import { type Dataset, type DatasetId, type Index, type RowTypes, validateDataset, validateIndex } from "./schema";

export type Load<T> =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "ready"; value: T };

const url = (file: string) => `${import.meta.env.BASE_URL}data/${file}`;

// One fetch per file per page load, shared by every component that asks.
const cache = new Map<string, Promise<unknown>>();

function fetchJSON(file: string): Promise<unknown> {
  let p = cache.get(file);
  if (!p) {
    p = fetch(url(file)).then(async (res) => {
      if (!res.ok) throw new Error(`${file}: HTTP ${res.status}`);
      return res.json();
    });
    p.catch(() => cache.delete(file));
    cache.set(file, p);
  }
  return p;
}

function useLoad<T>(file: string, parse: (raw: unknown) => T): Load<T> {
  const [load, setLoad] = useState<Load<T>>({ state: "loading" });
  useEffect(() => {
    let live = true;
    fetchJSON(file)
      .then((raw) => live && setLoad({ state: "ready", value: parse(raw) }))
      .catch((err: unknown) => live && setLoad({ state: "error", message: err instanceof Error ? err.message : String(err) }));
    return () => {
      live = false;
    };
    // parse is a module-level function for every caller, so file alone keys it.
  }, [file]);
  return load;
}

export function useDataset<K extends DatasetId>(id: K): Load<Dataset<RowTypes[K]>> {
  return useLoad(`${id}.json`, (raw) => validateDataset(id, raw));
}

export function useIndex(): Load<Index> {
  return useLoad("index.json", validateIndex);
}
