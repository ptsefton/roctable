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
