import { describe, it, expect } from "vitest";
import { loadCrate } from "../../lib/crate.js";
import { inspectCrate } from "../../lib/inspect.js";
import { extractTables } from "../../lib/extract.js";
import { tablesToCsvStrings } from "../../lib/csv.js";
import { selectAll } from "../helpers/select-all.js";

// Real-world fixture already checked into this repo (test_data/cooee):
// 8078 entities, 2439 of type Person, 1354 of type RepositoryObject, with
// RepositoryObject entities carrying a `ldac:mainText` file reference and an
// expandable `author` reference. Exercises the full inspect -> select ->
// extract -> csv pipeline end to end.
describe("cooee crate (test_data/cooee)", () => {
  it("extracts Person and RepositoryObject tables with the expected shape", () => {
    const { crate, crateDir } = loadCrate("test_data/cooee");
    const discovered = inspectCrate(crate);

    const config = {
      defaults: { max_repeat: 10 },
      tables: {
        Person: {
          properties: selectAll(discovered.potential_tables.Person.properties),
        },
        RepositoryObject: {
          load_text: "ldac:mainText",
          properties: selectAll(discovered.potential_tables.RepositoryObject.properties, {
            author: { include: true, expand: true },
          }),
        },
      },
      potential_tables: {},
    };

    const { tables } = extractTables(crate, config, { crateDir });

    expect(tables.Person.rows).toHaveLength(2439);
    expect(tables.RepositoryObject.rows).toHaveLength(1354);

    const first = tables.RepositoryObject.rows.find(
      (row) => row.identifier?.[0]?.name === "1-001",
    );
    // test_data/cooee only ships ro-crate-metadata.json, no payload files, so
    // load_text can't find data/1-001.txt on disk here — just confirm it
    // degrades to an empty string rather than throwing, and that the
    // metadata-derived columns (like the expanded author) are populated.
    expect(first["ldac:mainText"]).toEqual([{ name: "", id: null }]);
    expect(first.author_name).toBeDefined();

    const csv = tablesToCsvStrings({ tables });
    expect(csv.Person.split("\n")[0]).toContain("name");
    expect(csv.RepositoryObject).toContain("author_name");
  });
});
