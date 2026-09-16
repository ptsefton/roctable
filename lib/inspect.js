import { parse } from "csv-parse/sync";
import { defaultConfig } from "./config.js";
import { nodeFileReader } from "./io.js";

// Properties that are structural, not candidate output columns.
const STRUCTURAL_PROPS = new Set(["@id", "@type"]);

export function inspectCrate(crate) {
  const potential_tables = {};

  for (const entity of crate.entities()) {
    const entityTypes = entity["@type"] || [];
    for (const entityType of entityTypes) {
      if (!potential_tables[entityType]) {
        potential_tables[entityType] = { properties: {} };
      }
      const properties = potential_tables[entityType].properties;
      for (const prop of Object.keys(entity)) {
        if (STRUCTURAL_PROPS.has(prop)) continue;
        if (!properties[prop]) {
          properties[prop] = { include: false };
        }
      }
    }
  }

  return { potential_tables };
}

// Merge freshly discovered types/properties into an existing config without
// touching the user's existing include/expand/rename choices. A type already
// present under `tables` or `potential_tables` keeps its home; a newly seen
// type is added under `potential_tables`, unselected.
export function mergeDiscovered(existingConfig, discovered) {
  const config = existingConfig
    ? structuredClone(existingConfig)
    : defaultConfig();
  config.tables ??= {};
  config.potential_tables ??= {};
  config.defaults ??= defaultConfig().defaults;

  for (const [type, info] of Object.entries(discovered.potential_tables)) {
    const target = config.tables[type] || config.potential_tables[type];
    if (target) {
      target.properties ??= {};
      for (const prop of Object.keys(info.properties)) {
        if (!(prop in target.properties)) {
          target.properties[prop] = { include: false };
        }
      }
    } else {
      config.potential_tables[type] = {
        properties: structuredClone(info.properties),
      };
    }
  }

  return config;
}

// For every property configured with `expand: true` under `config.tables`,
// dereference its values across the crate and add/update a nested
// `properties` map listing every sub-property found — defaulted to
// `include: true` (opposite of the top-level discovery default), since the
// user has already opted into expanding this reference and is now pruning
// rather than selecting. Existing sub-property choices are preserved, same
// merge behaviour as mergeDiscovered.
export function discoverExpandedProperties(crate, config) {
  const next = structuredClone(config);

  for (const [entityType, tableConfig] of Object.entries(next.tables || {})) {
    for (const [prop, propConfig] of Object.entries(tableConfig.properties || {})) {
      if (!propConfig.expand) continue;

      const found = new Set();
      for (const entity of crate.entities()) {
        if (!(entity["@type"] || []).includes(entityType)) continue;
        for (const value of entity[prop] || []) {
          if (!value || typeof value !== "object" || !value["@id"]) continue;
          const target = crate.getEntity(value["@id"]);
          if (!target) continue;
          for (const exProp of Object.keys(target)) {
            if (!STRUCTURAL_PROPS.has(exProp)) found.add(exProp);
          }
        }
      }

      propConfig.properties ??= {};
      for (const exProp of found) {
        if (!(exProp in propConfig.properties)) {
          propConfig.properties[exProp] = { include: true };
        }
      }
    }
  }

  return next;
}

// Resolve a `join: "csv"` property's referenced file (relative to the crate
// directory) and parse it into CSV records, mirroring extract.js's own
// loadText()/parse() handling of the same property. Returns [] for an entity
// with no value or an unreadable file — discovery has nothing to report for
// those, extract.js's own warning covers the actual export.
async function readJoinRecords(prop, entity, fileReader) {
  const first = entity[prop]?.[0];
  const relPath = first && typeof first === "object" && first["@id"] ? first["@id"] : first;
  const text = relPath ? await fileReader.readFile(relPath) : null;
  if (!text) return [];
  return parse(text, { columns: true, skip_empty_lines: false });
}

// For every property configured with `join: "csv"` under `config.tables`,
// read the joined CSV for each matching entity and add/update a nested
// `columns` map listing every CSV header found — defaulted to
// `include: true`, the same pruning-not-selecting convention
// discoverExpandedProperties uses for `properties`, since the user has
// already opted into joining this CSV and is now choosing which columns to
// drop or expand rather than which to add.
//
// A column already marked `expand: true` (SPEC.md §5 — its values are @ids
// of another entity in the crate, e.g. a transcript's `speaker` column) is
// also dereferenced across every value seen, and its target entities' own
// properties are merged into that column's `properties` map, exactly as
// discoverExpandedProperties does for an `expand:true` top-level property.
// Async because reading the joined file goes through `fileReader`
// (lib/io.js's nodeFileReader(crateDir) by default), same as extract.js.
export async function discoverJoinColumns(crate, config, { crateDir, fileReader } = {}) {
  const reader = fileReader || nodeFileReader(crateDir);
  const next = structuredClone(config);

  for (const [entityType, tableConfig] of Object.entries(next.tables || {})) {
    for (const [prop, propConfig] of Object.entries(tableConfig.properties || {})) {
      if (propConfig.join !== "csv") continue;

      const headers = new Set();
      const expandTargets = {};

      for (const entity of crate.entities()) {
        if (!(entity["@type"] || []).includes(entityType)) continue;
        const records = await readJoinRecords(prop, entity, reader);
        for (const record of records) {
          for (const [header, value] of Object.entries(record)) {
            headers.add(header);
            if (!propConfig.columns?.[header]?.expand || !value) continue;
            const target = crate.getEntity(value);
            if (!target) continue;
            expandTargets[header] ??= new Set();
            for (const exProp of Object.keys(target)) {
              if (!STRUCTURAL_PROPS.has(exProp)) expandTargets[header].add(exProp);
            }
          }
        }
      }

      propConfig.columns ??= {};
      for (const header of headers) {
        if (!(header in propConfig.columns)) {
          propConfig.columns[header] = { include: true };
        }
      }
      for (const [header, found] of Object.entries(expandTargets)) {
        const colConfig = propConfig.columns[header];
        colConfig.properties ??= {};
        for (const exProp of found) {
          if (!(exProp in colConfig.properties)) {
            colConfig.properties[exProp] = { include: true };
          }
        }
      }
    }
  }

  return next;
}
