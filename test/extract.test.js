import path from "path";
import { fileURLToPath } from "url";
import { describe, it, expect, vi, afterEach } from "vitest";
import { buildCrate } from "./helpers/build-crate.js";
import { extractTables } from "../lib/extract.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function config(tables, defaults) {
  return { defaults: defaults || { max_repeat: 10 }, tables, potential_tables: {} };
}

describe("extractTables", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("only includes properties explicitly marked include:true", () => {
    const crate = buildCrate([
      { "@id": "#p1", "@type": "Person", name: "Alice", email: "a@example.com" },
    ]);
    const cfg = config({
      Person: { properties: { name: { include: true }, email: { include: false } } },
    });

    const { tables } = extractTables(crate, cfg);

    expect(tables.Person.rows).toHaveLength(1);
    expect(tables.Person.rows[0].name).toEqual([{ name: "Alice", id: null }]);
    expect(tables.Person.rows[0].email).toBeUndefined();
  });

  it("treats @id as a single identifier column, not a repeated/character-iterated one", () => {
    const crate = buildCrate([
      { "@id": "https://example.org/a-fairly-long-identifier-string", "@type": "Person", name: "Alice" },
    ]);
    const cfg = config({ Person: { properties: { name: { include: true } } } });

    const { tables } = extractTables(crate, cfg);

    expect(tables.Person.keys["@id"]).toBe(1);
    expect(tables.Person.rows[0]["@id"]).toEqual([
      { name: "https://example.org/a-fairly-long-identifier-string", id: null },
    ]);
  });

  it("applies rename as the output column stem", () => {
    const crate = buildCrate([{ "@id": "#p1", "@type": "Person", name: "Alice" }]);
    const cfg = config({
      Person: { properties: { name: { include: true, rename: "full_name" } } },
    });

    const { tables } = extractTables(crate, cfg);

    expect(tables.Person.rows[0].full_name).toEqual([{ name: "Alice", id: null }]);
    expect(tables.Person.rows[0].name).toBeUndefined();
  });

  it("writes an entity with multiple @types into every configured matching table", () => {
    const crate = buildCrate([
      { "@id": "#c1", "@type": ["Collection", "Special"], name: "Widgets" },
    ]);
    const cfg = config({
      Collection: { properties: { name: { include: true } } },
      Special: { properties: { name: { include: true } } },
    });

    const { tables } = extractTables(crate, cfg);

    expect(tables.Collection.rows).toHaveLength(1);
    expect(tables.Special.rows).toHaveLength(1);
  });

  it("marks a plain (non-expanded) @id-reference property as a ref so its id survives", () => {
    const crate = buildCrate([
      { "@id": "#p1", "@type": "Person", name: "Alice", affiliation: { "@id": "#org1" } },
      { "@id": "#org1", "@type": "Organization", name: "Acme" },
    ]);
    const cfg = config({
      Person: {
        properties: { name: { include: true }, affiliation: { include: true, expand: false } },
      },
    });

    const { tables } = extractTables(crate, cfg);

    expect(tables.Person.refs.affiliation).toBe(true);
    expect(tables.Person.rows[0].affiliation).toEqual([{ name: "Acme", id: "#org1" }]);
  });

  it("expands a one-hop @id reference into prefixed columns", () => {
    const crate = buildCrate([
      { "@id": "#p1", "@type": "Person", name: "Alice", affiliation: { "@id": "#org1" } },
      { "@id": "#org1", "@type": "Organization", name: "Acme", url: "https://acme.example" },
    ]);
    const cfg = config({
      Person: {
        properties: { name: { include: true }, affiliation: { include: true, expand: true } },
      },
    });

    const { tables } = extractTables(crate, cfg);

    expect(tables.Person.rows[0].affiliation_name).toEqual([{ name: "Acme", id: null }]);
    expect(tables.Person.rows[0].affiliation_url).toEqual([{ name: "https://acme.example", id: null }]);
    expect(tables.Person.rows[0].affiliation).toBeUndefined();
  });

  it("falls back to a plain reference when an expand target is missing from the graph", () => {
    const crate = buildCrate([
      { "@id": "#p1", "@type": "Person", name: "Alice", affiliation: { "@id": "#missing-org" } },
    ]);
    const cfg = config({
      Person: {
        properties: { name: { include: true }, affiliation: { include: true, expand: true } },
      },
    });

    const { tables } = extractTables(crate, cfg);

    expect(tables.Person.rows[0].affiliation).toEqual([{ name: "#missing-org", id: "#missing-org" }]);
    expect(tables.Person.rows[0].affiliation_name).toBeUndefined();
  });

  it("loads referenced file text for a property with load_text:true", () => {
    const crateDir = path.join(__dirname, "fixtures", "load-text");
    const crate = buildCrate([
      { "@id": "#ro1", "@type": "RepositoryObject", mainText: { "@id": "sample.txt" } },
    ]);
    const cfg = config({
      RepositoryObject: {
        properties: { mainText: { include: true, load_text: true } },
      },
    });

    const { tables } = extractTables(crate, cfg, { crateDir });

    expect(tables.RepositoryObject.rows[0].mainText[0].name).toContain("Lorem ipsum");
  });

  it("warns and returns empty text when a load_text file is missing", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const crate = buildCrate([
      { "@id": "#ro1", "@type": "RepositoryObject", mainText: { "@id": "does-not-exist.txt" } },
    ]);
    const cfg = config({
      RepositoryObject: { properties: { mainText: { include: true, load_text: true } } },
    });

    const { tables } = extractTables(crate, cfg, { crateDir: __dirname });

    expect(tables.RepositoryObject.rows[0].mainText).toEqual([{ name: "", id: null }]);
    expect(warn).toHaveBeenCalled();
  });

  it("joins a referenced CSV file, producing one output row per CSV row with the entity's other columns repeated (SPEC.md §5)", () => {
    const crateDir = path.join(__dirname, "fixtures", "join-csv");
    const crate = buildCrate([
      {
        "@id": "#ro1",
        "@type": "RepositoryObject",
        name: "Interview with Alice",
        "ldac:mainText": { "@id": "transcript.csv" },
      },
    ]);
    const cfg = config({
      RepositoryObject: {
        properties: {
          name: { include: true },
          "ldac:mainText": { include: true, load_text: true, join: "csv" },
        },
      },
    });

    const { tables } = extractTables(crate, cfg, { crateDir });

    expect(tables.RepositoryObject.rows).toHaveLength(2);
    // The joined property itself is replaced by the per-row _concat_ columns.
    expect(tables.RepositoryObject.rows[0]["ldac:mainText"]).toBeUndefined();
    // Every other included column is repeated unchanged across the generated rows.
    expect(tables.RepositoryObject.rows[0].name).toEqual([{ name: "Interview with Alice", id: null }]);
    expect(tables.RepositoryObject.rows[1].name).toEqual([{ name: "Interview with Alice", id: null }]);
    expect(tables.RepositoryObject.rows[0]._concat_speaker).toEqual([{ name: "A", id: null }]);
    expect(tables.RepositoryObject.rows[0]._concat_text).toEqual([{ name: "Hello", id: null }]);
    expect(tables.RepositoryObject.rows[1]._concat_speaker).toEqual([{ name: "B", id: null }]);
    expect(tables.RepositoryObject.rows[1]._concat_text).toEqual([{ name: "Hi there", id: null }]);
    // _concat_ID is unique per generated row.
    const ids = tables.RepositoryObject.rows.map((r) => r._concat_ID[0].name);
    expect(new Set(ids).size).toBe(2);
  });

  it("leaves an entity as a single row when its join property has no value", () => {
    const crateDir = path.join(__dirname, "fixtures", "join-csv");
    const crate = buildCrate([{ "@id": "#ro1", "@type": "RepositoryObject", name: "No transcript" }]);
    const cfg = config({
      RepositoryObject: {
        properties: {
          name: { include: true },
          "ldac:mainText": { include: true, load_text: true, join: "csv" },
        },
      },
    });

    const { tables } = extractTables(crate, cfg, { crateDir });

    expect(tables.RepositoryObject.rows).toHaveLength(1);
    expect(tables.RepositoryObject.rows[0]._concat_ID).toBeUndefined();
  });

  it("truncates a property beyond max_repeat and warns instead of dropping the column", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const crate = buildCrate([
      {
        "@id": "#ds1",
        "@type": "Dataset2",
        hasPart: [{ "@id": "#f1" }, { "@id": "#f2" }, { "@id": "#f3" }, { "@id": "#f4" }],
      },
    ]);
    const cfg = config(
      { Dataset2: { properties: { hasPart: { include: true } } } },
      { max_repeat: 2 },
    );

    const { tables } = extractTables(crate, cfg);

    expect(tables.Dataset2.keys.hasPart).toBe(2);
    expect(tables.Dataset2.rows[0].hasPart).toHaveLength(4);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("truncating to 2"));
  });
});
