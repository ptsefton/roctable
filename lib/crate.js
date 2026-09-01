import fs from "fs";
import path from "path";
import { ROCrate } from "ro-crate";

export function loadCrate(crateDir) {
  if (!fs.existsSync(crateDir) || !fs.lstatSync(crateDir).isDirectory()) {
    throw new Error(`${crateDir} is not a valid directory`);
  }
  const metadataFile = path.join(crateDir, "ro-crate-metadata.json");
  if (!fs.existsSync(metadataFile)) {
    throw new Error(`Metadata file not found in ${crateDir}`);
  }
  const metadata = JSON.parse(fs.readFileSync(metadataFile, "utf8"));
  const crate = new ROCrate(metadata, { link: true, array: true });
  return { crate, crateDir };
}
