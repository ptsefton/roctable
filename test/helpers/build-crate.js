import { ROCrate } from "ro-crate";

// Build an in-memory crate from plain entity objects, skipping the disk I/O
// that lib/crate.js's loadCrate needs. The constructor auto-creates a root
// Dataset + metadata descriptor; avoid asserting on those unless a test is
// specifically about the root dataset.
export function buildCrate(entities) {
  const crate = new ROCrate({}, { link: true, array: true });
  for (const entity of entities) {
    crate.addEntity(entity);
  }
  return crate;
}
