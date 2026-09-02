import { describe, it, expect } from "vitest";
import { buildCrate } from "./helpers/build-crate.js";
import { inspectCrate, mergeDiscovered, discoverExpandedProperties } from "../lib/inspect.js";

describe("inspectCrate", () => {
  it("discovers every @type as a potential table with the union of observed properties", () => {
    const crate = buildCrate([
      { "@id": "#p1", "@type": "Person", name: "Alice", email: "a@example.com" },
      { "@id": "#p2", "@type": "Person", name: "Bob" },
      { "@id": "#org1", "@type": "Organization", name: "Acme" },
    ]);

    const { potential_tables } = inspectCrate(crate);

    expect(Object.keys(potential_tables.Person.properties).sort()).toEqual(["email", "name"]);
    expect(Object.keys(potential_tables.Organization.properties).sort()).toEqual(["name"]);
    for (const prop of Object.values(potential_tables.Person.properties)) {
      expect(prop.include).toBe(false);
    }
  });

  it("does not list @id or @type as candidate properties", () => {
    const crate = buildCrate([{ "@id": "#p1", "@type": "Person", name: "Alice" }]);
    const { potential_tables } = inspectCrate(crate);
    expect(potential_tables.Person.properties["@id"]).toBeUndefined();
    expect(potential_tables.Person.properties["@type"]).toBeUndefined();
  });

  it("puts every distinct @type an entity carries into its own potential table", () => {
    const crate = buildCrate([
      { "@id": "#c1", "@type": ["Collection", "Special"], name: "Widgets" },
    ]);
    const { potential_tables } = inspectCrate(crate);
    expect(potential_tables.Collection.properties.name).toBeDefined();
    expect(potential_tables.Special.properties.name).toBeDefined();
  });
});

describe("mergeDiscovered", () => {
  it("adds newly seen properties to an already-selected table without touching existing choices", () => {
    const existing = {
      defaults: { max_repeat: 10 },
      tables: {
        Person: { properties: { name: { include: true, rename: "full_name" } } },
      },
      potential_tables: {},
    };
    const discovered = {
      potential_tables: {
        Person: { properties: { name: { include: false }, email: { include: false } } },
      },
    };

    const merged = mergeDiscovered(existing, discovered);

    expect(merged.tables.Person.properties.name).toEqual({ include: true, rename: "full_name" });
    expect(merged.tables.Person.properties.email).toEqual({ include: false });
  });

  it("adds a newly seen type under potential_tables, not tables", () => {
    const existing = { defaults: { max_repeat: 10 }, tables: {}, potential_tables: {} };
    const discovered = {
      potential_tables: { Organization: { properties: { name: { include: false } } } },
    };

    const merged = mergeDiscovered(existing, discovered);

    expect(merged.tables.Organization).toBeUndefined();
    expect(merged.potential_tables.Organization.properties.name).toEqual({ include: false });
  });

  it("leaves an already-selected potential table's properties alone if rediscovered identically", () => {
    const existing = {
      defaults: { max_repeat: 10 },
      tables: {},
      potential_tables: { Organization: { properties: { name: { include: true } } } },
    };
    const discovered = {
      potential_tables: { Organization: { properties: { name: { include: false } } } },
    };

    const merged = mergeDiscovered(existing, discovered);

    expect(merged.potential_tables.Organization.properties.name).toEqual({ include: true });
  });

  it("builds a fresh config from scratch when no existing config is passed", () => {
    const discovered = {
      potential_tables: { Person: { properties: { name: { include: false } } } },
    };
    const merged = mergeDiscovered(null, discovered);
    expect(merged.potential_tables.Person.properties.name).toEqual({ include: false });
    expect(merged.tables).toEqual({});
  });
});

describe("discoverExpandedProperties", () => {
  it("lists every sub-property found through an expand:true property, defaulted to include:true", () => {
    const crate = buildCrate([
      { "@id": "#p1", "@type": "Person", name: "Alice", affiliation: { "@id": "#org1" } },
      { "@id": "#org1", "@type": "Organization", name: "Acme", url: "https://acme.example" },
    ]);
    const config = {
      defaults: { max_repeat: 10 },
      tables: {
        Person: {
          properties: {
            name: { include: true },
            affiliation: { include: true, expand: true },
          },
        },
      },
      potential_tables: {},
    };

    const result = discoverExpandedProperties(crate, config);

    expect(result.tables.Person.properties.affiliation.properties).toEqual({
      name: { include: true },
      url: { include: true },
    });
  });

  it("does not touch properties without expand:true", () => {
    const crate = buildCrate([{ "@id": "#p1", "@type": "Person", name: "Alice" }]);
    const config = {
      defaults: { max_repeat: 10 },
      tables: { Person: { properties: { name: { include: true } } } },
      potential_tables: {},
    };

    const result = discoverExpandedProperties(crate, config);

    expect(result.tables.Person.properties.name.properties).toBeUndefined();
  });

  it("preserves an existing sub-property choice on re-discovery instead of resetting it to true", () => {
    const crate = buildCrate([
      { "@id": "#p1", "@type": "Person", name: "Alice", affiliation: { "@id": "#org1" } },
      { "@id": "#org1", "@type": "Organization", name: "Acme", url: "https://acme.example" },
    ]);
    const config = {
      defaults: { max_repeat: 10 },
      tables: {
        Person: {
          properties: {
            affiliation: {
              include: true,
              expand: true,
              properties: { url: { include: false } },
            },
          },
        },
      },
      potential_tables: {},
    };

    const result = discoverExpandedProperties(crate, config);

    expect(result.tables.Person.properties.affiliation.properties).toEqual({
      url: { include: false },
      name: { include: true },
    });
  });
});
