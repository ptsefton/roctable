#!/usr/bin/env node
// Download the Farms to Freeways RO-Crate and unpack it into ./f2f, for
// trying roctable against a real crate with CSV transcripts to `join`
// (examples/f2f-config.json). This is a large download (~1.6GB).
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execFileSync } from "child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.join(__dirname, "..");
const url =
  "https://data.ldaca.edu.au/api/object/arcp%3A%2F%2Fname%2Cdoi10.4225%252F35%252F555d661071c76.zip";
const zipPath = path.join(repoRoot, "f2f.zip");
const outDir = path.join(repoRoot, "f2f");

console.log(`Downloading ${url} (this is a large file, ~1.6GB)`);
const response = await fetch(url);
if (!response.ok) {
  throw new Error(`Download failed: ${response.status} ${response.statusText}`);
}
fs.writeFileSync(zipPath, Buffer.from(await response.arrayBuffer()));
console.log(`Saved ${zipPath}`);

console.log(`Unzipping into ${outDir}`);
execFileSync("unzip", ["-o", zipPath, "-d", outDir], { stdio: "inherit" });
