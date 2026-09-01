import fs from "fs";
import path from "path";
import { stringify } from "csv-stringify/sync";

function cleanTextForCSV(text) {
  if (text == null) return "";
  return String(text).replace(/[\r\n]+/g, "\\n").trim();
}

// Build the flat column list for a table: one {header, prop, index, field}
// entry per (property, repeat-index, name|id) slot. First occurrence of a
// repeated property is unsuffixed, subsequent ones are `_1`, `_2`, ...
function buildColumns(table) {
  const columns = [];
  for (const prop of Object.keys(table.keys)) {
    const count = table.keys[prop];
    const hasRefs = !!table.refs[prop];
    for (let i = 0; i < count; i++) {
      const suffix = i === 0 ? "" : `_${i}`;
      columns.push({ header: `${prop}${suffix}`, prop, index: i, field: "name" });
      if (hasRefs) {
        columns.push({ header: `${prop}_id${suffix}`, prop, index: i, field: "id" });
      }
    }
  }
  return columns;
}

// Pure: in-memory tables (as produced by lib/extract.js) -> { [tableName]: csvString }.
export function tablesToCsvStrings(data) {
  const result = {};
  for (const [tableName, table] of Object.entries(data.tables)) {
    const columns = buildColumns(table);
    const headerRow = columns.map((c) => c.header);
    const rows = table.rows.map((row) =>
      columns.map((c) => cleanTextForCSV(row[c.prop]?.[c.index]?.[c.field])),
    );
    result[tableName] = stringify([headerRow, ...rows]);
  }
  return result;
}

export function writeCsvFiles(strings, outDir, crateName) {
  fs.mkdirSync(outDir, { recursive: true });
  const written = [];
  for (const [tableName, csv] of Object.entries(strings)) {
    const filePath = path.join(outDir, `${crateName}_${tableName}.csv`);
    fs.writeFileSync(filePath, csv);
    written.push(filePath);
  }
  return written;
}
