// Copies the committed report PDF and figures into public/docs so the site can
// link to them without a second copy living in git. Runs before dev and build.
import { cpSync, existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const web = join(dirname(fileURLToPath(import.meta.url)), "..");
const docs = join(web, "..", "docs");
const out = join(web, "public", "docs");

rmSync(out, { recursive: true, force: true });
mkdirSync(join(out, "images"), { recursive: true });
const pdf = join(docs, "report.pdf");
if (!existsSync(pdf)) throw new Error(`missing ${pdf}`);
cpSync(pdf, join(out, "report.pdf"));
for (const f of readdirSync(join(docs, "images"))) {
  if (f.endsWith(".png")) cpSync(join(docs, "images", f), join(out, "images", f));
}
console.log("copied report.pdf and figures into public/docs");
