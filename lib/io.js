import fs from "fs";
import path from "path";

// The default file reader extractTables() uses when no `fileReader` is
// passed in — Node fs, resolving each path relative to `crateDir`. This is
// the CLI's own path (bin/roctable.js never passes a fileReader). A
// non-Node embedder — e.g. a browser app using the File System Access API,
// which has no `fs` and no synchronous reads — passes its own `{ readFile }`
// instead. See lib/extract.js's loadText() and SPEC.md's file I/O section.
export function nodeFileReader(crateDir) {
  return {
    async readFile(relPath) {
      const fullPath = crateDir ? path.join(crateDir, relPath) : relPath;
      if (!fs.existsSync(fullPath)) return null;
      return fs.readFileSync(fullPath, "utf8");
    },
  };
}
