import { parse } from "csv-parse/sync";
import { getMaxRepeat } from "./config.js";
import { nodeFileReader } from "./io.js";

function resolveValueName(value) {
  return value["name"]?.join(",") || value["rdfs:label"]?.join(",") || value["@id"];
}

function bumpKey(table, prop, count) {
  table.keys[prop] = Math.max(table.keys[prop] || 0, count);
}

// Dereference `expandEntity` (one hop) and flatten its own properties into
// `row`/`table` as `${prop}_${exProp}` columns. `expandConfig` is the
// expanded property's own nested `properties` map (SPEC.md §5) — when
// present, a sub-property is skipped only if explicitly `include: false`;
// a sub-property inspect hasn't seen yet (absent from the map) still comes
// through, so newly-appeared crate properties aren't silently dropped
// before the next `inspect` run.
function expandInto(row, table, prop, expandEntity, expandConfig) {
  for (const exProp of Object.keys(expandEntity)) {
    if (exProp === "@id" || exProp === "@type") continue;
    if (expandConfig?.[exProp]?.include === false) continue;
    const outputExProp = expandConfig?.[exProp]?.rename || exProp;
    const expandPropName = `${prop}_${outputExProp}`;
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

// Resolve a `load_text` property's value to a file — relative to the crate
// directory — and read its contents via `fileReader`, an injected
// `{ readFile(relPath) }` (lib/io.js's nodeFileReader by default, or a
// non-Node embedder's own implementation). Returns the referenced entity's
// @id too, when there was one, for use in join's `_concat_ID`.
async function loadText(prop, entity, fileReader) {
  const first = entity[prop]?.[0];
  const isRef = first && typeof first === "object" && first["@id"];
  const relPath = isRef ? first["@id"] : first;
  const fileId = isRef ? first["@id"] : null;
  const text = relPath ? await fileReader.readFile(relPath) : null;
  if (text == null) {
    console.warn(
      `Warning: load_text could not find file for property '${prop}' on entity '${entity["@id"]}': ${relPath}`,
    );
    return { text: "", fileId };
  }
  return { text, fileId };
}

// Cross a set of base rows with one join's CSV records: each record becomes
// its own row, carrying every base row's columns forward unchanged plus a
// `_concat_ID` and one `_concat_${header}` per CSV column (SPEC.md §5).
function joinRecords(rows, entityId, outputProp, fileId, records, table) {
  const joined = [];
  records.forEach((record, index) => {
    const concatId = `${entityId}::${outputProp}::${fileId ?? ""}::${index}`;
    for (const base of rows) {
      const row = { ...base, _concat_ID: [{ name: concatId, id: null }] };
      bumpKey(table, "_concat_ID", 1);
      for (const [header, value] of Object.entries(record)) {
        const colName = `_concat_${header}`;
        row[colName] = [{ name: value, id: null }];
        bumpKey(table, colName, 1);
      }
      joined.push(row);
    }
  });
  return joined;
}

// Walk `crate` and build one in-memory table per entry in `config.tables`.
// Returns { tables: { [tableName]: { rows, keys, refs } } }, the structure
// lib/csv.js turns into CSV text. Async because load_text properties read a
// file through `fileReader` (lib/io.js's nodeFileReader(crateDir) by
// default) — everything else here is synchronous crate-walking, but the one
// await needed for that case makes the whole function async.
export async function extractTables(crate, config, { crateDir, fileReader } = {}) {
  const reader = fileReader || nodeFileReader(crateDir);
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
      const joins = [];

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

        if (propConfig.load_text) {
          const { text, fileId } = await loadText(prop, entity, reader);
          if (propConfig.join === "csv") {
            const records = text ? parse(text, { columns: true, skip_empty_lines: false }) : [];
            joins.push({ outputProp, fileId, records });
          } else {
            row[outputProp] = [{ name: text, id: null }];
            bumpKey(table, outputProp, 1);
          }
          continue;
        }

        const newValues = [];
        for (const value of entity[prop]) {
          if (value && typeof value === "object" && value["@id"]) {
            if (propConfig.expand) {
              const expandEntity = crate.getEntity(value["@id"]);
              if (expandEntity) {
                expandInto(row, table, outputProp, expandEntity, propConfig.properties);
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

      let rows = [row];
      for (const { outputProp, fileId, records } of joins) {
        rows = joinRecords(rows, entity["@id"], outputProp, fileId, records, table);
      }
      table.rows.push(...rows);
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
