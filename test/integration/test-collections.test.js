import fs from "fs";
import { describe, it, expect } from "vitest";
import { loadCrate } from "../../lib/crate.js";
import { inspectCrate } from "../../lib/inspect.js";
import { extractTables } from "../../lib/extract.js";
import { tablesToCsvStrings } from "../../lib/csv.js";
import { selectAll } from "../helpers/select-all.js";

const crateDir =
  "test-collections/access-test-files/UDHR-Translations-w-SubCollections";

// git submodule fixture (test-collections). Skip cleanly if it hasn't been
// checked out (e.g. a shallow clone of this repo without --recurse-submodules).
const submoduleCheckedOut = fs.existsSync(`${crateDir}/ro-crate-metadata.json`);
const maybeDescribe = submoduleCheckedOut ? describe : describe.skip;

// This crate carries multi-@type entities (File+ldac:PrimaryMaterial,
// Dataset+RepositoryCollection) and nested RepositoryCollections, unlike
// test_data/cooee's flatter structure.
maybeDescribe("UDHR-Translations-w-SubCollections (test-collections submodule)", () => {
  it("extracts Person and RepositoryObject tables at their known sizes", () => {
    const { crate, crateDir: resolvedDir } = loadCrate(crateDir);
    const discovered = inspectCrate(crate);

    const config = {
      defaults: { max_repeat: 10 },
      tables: {
        Person: { properties: selectAll(discovered.potential_tables.Person.properties) },
        RepositoryObject: {
          properties: selectAll(discovered.potential_tables.RepositoryObject.properties),
        },
      },
      potential_tables: {},
    };

    const { tables } = extractTables(crate, config, { crateDir: resolvedDir });

    expect(tables.Person.rows).toHaveLength(9);
    expect(tables.RepositoryObject.rows).toHaveLength(12);
  });

  it("writes a File entity into the File table for every entity carrying that @type, including multi-typed ones", () => {
    const { crate, crateDir: resolvedDir } = loadCrate(crateDir);
    const discovered = inspectCrate(crate);

    const config = {
      defaults: { max_repeat: 10 },
      tables: {
        File: { properties: selectAll(discovered.potential_tables.File.properties) },
      },
      potential_tables: {},
    };

    const { tables } = extractTables(crate, config, { crateDir: resolvedDir });

    // File,ldac:CollectionProtocol (1) + File,ldac:PrimaryMaterial (22)
    expect(tables.File.rows).toHaveLength(23);

    const csv = tablesToCsvStrings({ tables });
    expect(csv.File.split("\n")[0]).toContain("name");
  });
});
