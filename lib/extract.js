import fs from "fs";
import path from "path";
import { getMaxRepeat } from "./config.js";

function resolveValueName(value) {
  return value["name"]?.join(",") || value["rdfs:label"]?.join(",") || value["@id"];
}

function bumpKey(table, prop, count) {
  table.keys[prop] = Math.max(table.keys[prop] || 0, count);
}

// Dereference `expandEntity` (one hop) and flatten its own properties into
// `row`/`table` as `${prop}_${exProp}` columns.
function expandInto(row, table, prop, expandEntity) {
  for (const exProp of Object.keys(expandEntity)) {
    if (exProp === "@id" || exProp === "@type") continue;
    const expandPropName = `${prop}_${exProp}`;
    const expandValues = [];
    for (const expandValue of expandEntity[exProp]) {
      if (expandValue && typeof expandValue === "object" && expandValue["@id"]) {
        table.refs[expandPropName] = true;
        expandValues.push({ name: resolveValueName(expandValue), id: expandValue["@id"] });
      } else {
        expandValues.push({ name: expandValue, id: null });
      }
    }
    row[expandPropName] = expandValues;
    bumpKey(table, expandPropName, expandValues.length);
  }
}

function loadText(tableConfig, prop, entity, crateDir) {
  const values = entity[prop];
  const first = values?.[0];
  const relPath = first && typeof first === "object" && first["@id"] ? first["@id"] : first;
  const fullPath = relPath && crateDir ? path.join(crateDir, relPath) : relPath;
  if (fullPath && fs.existsSync(fullPath)) {
    return fs.readFileSync(fullPath, "utf8");
  }
  console.warn(
    `Warning: load_text could not find file for property '${prop}' on entity '${entity["@id"]}': ${fullPath}`,
  );
  return "";
}

// Walk `crate` and build one in-memory table per entry in `config.tables`.
// Returns { tables: { [tableName]: { rows, keys, refs } } }, the structure
// lib/csv.js turns into CSV text.
export function extractTables(crate, config, { crateDir } = {}) {
  const data = { tables: {} };
  for (const tableName of Object.keys(config.tables || {})) {
    data.tables[tableName] = { rows: [], keys: {}, refs: {} };
  }

  for (const entity of crate.entities()) {
    const entityTypes = entity["@type"] || [];
    for (const entityType of entityTypes) {
      const tableConfig = config.tables?.[entityType];
      if (!tableConfig) continue;

      const table = data.tables[entityType];
      const properties = tableConfig.properties || {};
      const row = {};

      for (const prop of Object.keys(entity)) {
        if (prop === "@type") continue;
        const propConfig = prop === "@id" ? { include: true } : properties[prop];
        if (!propConfig?.include) continue;
        const outputProp = propConfig.rename || prop;

        // @id is a single string, not an array-valued property like the rest —
        // handle it directly rather than iterating it as if it were a list of values.
        if (prop === "@id") {
          row[outputProp] = [{ name: entity["@id"], id: null }];
          bumpKey(table, outputProp, 1);
          continue;
        }

        if (tableConfig.load_text === prop) {
          row[outputProp] = [{ name: loadText(tableConfig, prop, entity, crateDir), id: null }];
          bumpKey(table, outputProp, 1);
          continue;
        }

        const newValues = [];
        for (const value of entity[prop]) {
          if (value && typeof value === "object" && value["@id"]) {
            if (propConfig.expand) {
              const expandEntity = crate.getEntity(value["@id"]);
              if (expandEntity) {
                expandInto(row, table, outputProp, expandEntity);
                continue;
              }
              // No target entity found for expansion: fall back to a plain reference below.
            }
            newValues.push({ name: resolveValueName(value), id: value["@id"] });
          } else {
            newValues.push({ name: value, id: null });
          }
        }

        if (newValues.length) {
          if (newValues.some((v) => v.id != null)) {
            table.refs[outputProp] = true;
          }
          row[outputProp] = newValues;
          bumpKey(table, outputProp, newValues.length);
        }
      }

      table.rows.push(row);
    }
  }

  applyMaxRepeat(data, config);
  return data;
}

// Cap each column at its table's configured max_repeat, warning once per
// property that gets truncated. Values beyond the cap are dropped rather
// than spilling into a separate junction table.
function applyMaxRepeat(data, config) {
  for (const [tableName, table] of Object.entries(data.tables)) {
    const maxRepeat = getMaxRepeat(config, tableName);
    for (const prop of Object.keys(table.keys)) {
      if (table.keys[prop] > maxRepeat) {
        console.warn(
          `Warning: property '${prop}' on table '${tableName}' has up to ${table.keys[prop]} values per row; truncating to ${maxRepeat}.`,
        );
        table.keys[prop] = maxRepeat;
      }
    }
  }
}
