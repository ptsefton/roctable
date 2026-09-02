#!/usr/bin/env node
// Download the COOEE (Corpus of Oz Early English) RO-Crate and unpack it into
// ./cooee, for trying roctable against a real crate (examples/cooee-config.json).
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execFileSync } from "child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.join(__dirname, "..");
const url =
  "https://data.ldaca.edu.au/api/object/arcp%3A%2F%2Fname%2Chdl10.26180~23961609.zip";
const zipPath = path.join(repoRoot, "cooee.zip");
const outDir = path.join(repoRoot, "cooee");

console.log(`Downloading ${url}`);
const response = await fetch(url);
if (!response.ok) {
  throw new Error(`Download failed: ${response.status} ${response.statusText}`);
}
fs.writeFileSync(zipPath, Buffer.from(await response.arrayBuffer()));
console.log(`Saved ${zipPath}`);

console.log(`Unzipping into ${outDir}`);
execFileSync("unzip", ["-o", zipPath, "-d", outDir], { stdio: "inherit" });
