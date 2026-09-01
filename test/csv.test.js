import os from "os";
import fs from "fs";
import path from "path";
import { describe, it, expect, afterEach } from "vitest";
import { tablesToCsvStrings, writeCsvFiles } from "../lib/csv.js";

describe("tablesToCsvStrings", () => {
  it("emits a plain header for a single-valued, non-ref property", () => {
    const data = {
      tables: {
        Person: {
          keys: { name: 1 },
          refs: {},
          rows: [{ name: [{ name: "Alice", id: null }] }],
        },
      },
    };

    const csv = tablesToCsvStrings(data).Person;

    expect(csv).toBe("name\nAlice\n");
  });

  it("pairs value/_id columns for a ref property, and numbers repeats from the second occurrence", () => {
    const data = {
      tables: {
        Dataset: {
          keys: { hasPart: 2 },
          refs: { hasPart: true },
          rows: [
            {
              hasPart: [
                { name: "File A", id: "#f1" },
                { name: "File B", id: "#f2" },
              ],
            },
          ],
        },
      },
    };

    const csv = tablesToCsvStrings(data).Dataset;
    const [header, row] = csv.trim().split("\n");

    expect(header).toBe("hasPart,hasPart_id,hasPart_1,hasPart_id_1");
    expect(row).toBe("File A,#f1,File B,#f2");
  });

  it("leaves a missing repeat slot blank rather than shifting later columns", () => {
    const data = {
      tables: {
        Dataset: {
          keys: { hasPart: 2 },
          refs: {},
          rows: [{ hasPart: [{ name: "Only one", id: null }] }],
        },
      },
    };

    const csv = tablesToCsvStrings(data).Dataset;
    const [, row] = csv.trim().split("\n");

    expect(row).toBe("Only one,");
  });

  it("escapes embedded newlines so rows stay one CSV line each", () => {
    const data = {
      tables: {
        Person: {
          keys: { bio: 1 },
          refs: {},
          rows: [{ bio: [{ name: "Line one\nLine two", id: null }] }],
        },
      },
    };

    const csv = tablesToCsvStrings(data).Person;

    expect(csv.split("\n")).toHaveLength(3); // header, one data row, trailing newline
    expect(csv).toContain("Line one\\nLine two");
  });
});

describe("writeCsvFiles", () => {
  let tmpDir;

  afterEach(() => {
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("writes one file per table, named {crateName}_{table}.csv", () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "roctable-csv-"));
    const written = writeCsvFiles({ Person: "name\nAlice\n" }, tmpDir, "mycrate");

    expect(written).toEqual([path.join(tmpDir, "mycrate_Person.csv")]);
    expect(fs.readFileSync(written[0], "utf8")).toBe("name\nAlice\n");
  });
});
