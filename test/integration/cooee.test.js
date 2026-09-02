import fs from "fs";
import { describe, it, expect } from "vitest";
import { loadCrate } from "../../lib/crate.js";
import { inspectCrate } from "../../lib/inspect.js";
import { extractTables } from "../../lib/extract.js";
import { tablesToCsvStrings } from "../../lib/csv.js";
import { selectAll } from "../helpers/select-all.js";

const crateDir = "cooee";

// Real-world fixture, fetched by `npm run download:cooee` into ./cooee
// (not checked into git — see .gitignore). Skip cleanly if it hasn't been
// downloaded. 8078 entities, 2439 of type Person, 1354 of type
// RepositoryObject, with RepositoryObject entities carrying a `ldac:mainText`
// file reference (with the actual transcript text on disk, unlike the old
// metadata-only stub this replaced) and an expandable `author` reference.
// Exercises the full inspect -> select -> extract -> csv pipeline end to end.
const downloaded = fs.existsSync(`${crateDir}/ro-crate-metadata.json`);
const maybeDescribe = downloaded ? describe : describe.skip;

maybeDescribe("cooee crate (downloaded via npm run download:cooee)", () => {
  it("extracts Person and RepositoryObject tables with the expected shape", async () => {
    const { crate, crateDir: resolvedDir } = loadCrate(crateDir);
    const discovered = inspectCrate(crate);

    const config = {
      defaults: { max_repeat: 10 },
      tables: {
        Person: {
          properties: selectAll(discovered.potential_tables.Person.properties),
        },
        RepositoryObject: {
          properties: selectAll(discovered.potential_tables.RepositoryObject.properties, {
            author: { include: true, expand: true },
            "ldac:mainText": { include: true, load_text: true },
          }),
        },
      },
      potential_tables: {},
    };

    const { tables } = await extractTables(crate, config, { crateDir: resolvedDir });

    expect(tables.Person.rows).toHaveLength(2439);
    expect(tables.RepositoryObject.rows).toHaveLength(1354);

    const first = tables.RepositoryObject.rows.find(
      (row) => row.identifier?.[0]?.name === "1-001",
    );
    // Unlike the old metadata-only stub, the downloaded crate ships its
    // actual transcript files, so load_text should find real content here.
    expect(first["ldac:mainText"][0].name).toContain("Dear Sir");
    expect(first.author_name).toBeDefined();

    const csv = tablesToCsvStrings({ tables });
    expect(csv.Person.split("\n")[0]).toContain("name");
    expect(csv.RepositoryObject).toContain("author_name");
  });
});
