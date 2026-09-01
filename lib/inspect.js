import { defaultConfig } from "./config.js";

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
