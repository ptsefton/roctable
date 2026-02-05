import fs from "fs";
import path from "path";
import { Command } from "commander";
import { fileURLToPath } from "url";
import { ROCrate } from "ro-crate";
import { stringify } from "csv-stringify/sync";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const program = new Command();

program
  .name("load_ro_crate")
  .description("Load an RO-Crate from a specified directory")
  .argument("<path_to_crate_directory>", "Path to the crate directory")
  .option(
    "-c, --config <configPath>",
    "Path to the config file",
    path.join(__dirname, "lib", "default_config.json"),
  )

  .action(async (cratePath, options) => {
    if (!fs.existsSync(cratePath) || !fs.lstatSync(cratePath).isDirectory()) {
      console.error(`Error: ${cratePath} is not a valid directory`);
      return;
    }
    const metadataFile = path.join(cratePath, "ro-crate-metadata.json");
    if (!fs.existsSync(metadataFile)) {
      console.error(`Error: Metadata file not found in ${cratePath}`);
      return;
    }
    const metadata = JSON.parse(fs.readFileSync(metadataFile, "utf8"));
    const crate = new ROCrate(metadata, { link: true, array: true });
    const config = JSON.parse(fs.readFileSync(options.config, "utf8"));
    
    /**
     * Data structure for storing table data before CSV serialization:
     * 
     * data = {
     *   tables: {
     *     [entityType]: {
     *       rows: [
     *         { 
     *           [propName]: [{name: "value", id: "@id or null"}, ...],  // Array of value objects
     *           [propName]: [{name: "value", id: "@id or null"}, ...],
     *           ...
     *         },
     *         ...
     *       ],
     *       keys: { 
     *         [propName]: maxArrayLength,  // Tracks max array length for each property across all rows
     *         ...
     *       },
     *       refs: { 
     *         [propName]: true,  // Flags properties that contain at least one @id reference
     *         ...
     *       }
     *     },
     *     ...
     *   }
     * }
     * 
     * FIRST PASS: Build table structures with array values
     * - Each entity becomes a row in its corresponding table
     * - Each property stores an array of {name, id} objects
     * - keys[prop] tracks the maximum array length for that property
     * - refs[prop] flags if the property contains any references
     * 
     * SECOND PASS: Flatten arrays to CSV
     * - Properties with refs get paired columns: prop, prop_id, prop, prop_id, ...
     * - Properties without refs get repeated columns: prop, prop, prop, ...
     * - Array values are distributed across the repeated columns
     */
    const data = { tables: {} };

    // Initialize tables based on config
    if (config.tables) {
      for (let tableName in config.tables) {
        data.tables[tableName] = { rows: [], keys: {}, refs: {} };
      }
    }

    // ============================================================================
    // FIRST PASS: Build table structures with array values
    // ============================================================================
    // Process each entity in the RO-Crate and populate the data structure.
    // Each entity property is stored as an array of {name, id} objects to handle
    // multi-valued properties. This intermediate structure is then flattened
    // into CSV format in the second pass.
    
    for (let entity of crate.entities()) {
      // Loop through entity types and check if they're in config
      for (let entityType of entity["@type"]) {
        if (config.tables && config.tables[entityType]) {
          const row = {};
          const props = Object.keys(entity);
          
          for (let prop of props) {
            const values = entity[prop];
            if (!data.tables[entityType].keys[prop]) {
              data.tables[entityType].keys[prop] = 0;
            }
            if (values.length > data.tables[entityType].keys[prop]) {
              data.tables[entityType].keys[prop] = values.length;
            }
            const newValues = [];

            if (config.tables[entityType]?.load_text === prop) {
              // If this property is designated as the main text use the first value as a filename to load text content from (tho it may be an @id reference to a file entity, so we need to resolve it
              const loadTextValue = values[0];
              let textContent = "";
              let filePath = null;
              if (typeof loadTextValue === "object" && loadTextValue["@id"]) {
                filePath = loadTextValue["@id"];
              }else {
                filePath = loadTextValue;
              }
              filePath = path.join(cratePath, filePath);
              if (filePath && fs.existsSync(filePath)) {
                textContent = fs.readFileSync(filePath, "utf8");
                newValues.push({ name: textContent, id: null });
              }
            } else {
              for (const value of values) {
              if (typeof value === "object" && value["@id"]) {
                // Expand properties  
                if (config.tables[entityType]?.expand_props?.includes(prop)) {
                  const expandEntity = crate.getEntity(value["@id"]);
                  if (expandEntity) {
                    // Expand / flatten the properties of the referenced entity into new columns with a prefix
                    for (const exProp of Object.keys(expandEntity)) {
                      const expandPropName = `${prop}_${exProp}`;
                      const expandValues = expandEntity[exProp];
                      if(exProp === "@id") continue; // Don't expand the @id prop itself as a column, we'll capture it in the _id column for the base prop
                      if (!data.tables[entityType].keys[expandPropName]) {
                        data.tables[entityType].keys[expandPropName] = 0;
                      }
                      if (
                        expandValues.length >
                        data.tables[entityType].keys[expandPropName]
                      ) {
                        data.tables[entityType].keys[expandPropName] =
                          expandValues.length;
                      }
                      const expandedValues = [];

                      for (let expandValue of expandValues) {
                        if (
                          typeof expandValue === "object" &&
                          expandValue["@id"]
                        ) {
                           data.tables[entityType].refs[expandPropName] = true;
                          expandedValues.push({
                            name:
                              expandValue["name"]?.join(",") ||
                              expandValue["rdsfs:label"]?.join(",") ||
                              expandValue["@id"],
                            id: expandValue["@id"],
                          });
                        } else {
                          expandedValues.push({ name: expandValue, id: null });
                        }
                      }
                      
                      data.tables[entityType].refs[prop] = true;
                      row[expandPropName] = expandedValues;
                      //console.log(`Expanded property '${prop}' into '${expandPropName}' with values:`, expandedValues);
                    }
                    continue; // Skip adding the original prop below
                  }
                }
                newValues.push({
                  name:
                    value["name"]?.join(",") ||
                    value["rdsfs:label"]?.join(",") ||
                    value["@id"],
                  id: value["@id"],
                });
              } else {
                newValues.push({ name: value, id: null });
              }
            }
            }
            row[prop] = newValues;
          }

          data.tables[entityType].rows.push(row);
        }
      }
    }

    // Helper function to escape/clean text for CSV
    const cleanTextForCSV = (text) => {
      if (text == null) return "";
      const str = String(text);
      // Replace newlines and carriage returns with \n
      return str.replace(/[\r\n]+/g, "\\n").trim();
    };

    // ============================================================================
    // SECOND PASS: Flatten array values into CSV format
    // ============================================================================
    // Convert the table structures (with array values) into flat CSV files.
    // For each property:
    // - If it contains references (refs[prop] = true): create paired columns 
    //   like "prop, prop_id, prop, prop_id" to store both the display name and @id
    // - If it doesn't contain references: create repeated columns like "prop, prop, prop"
    // - Distribute array values across the repeated columns
    // - Properties with >5 values are skipped with a warning to avoid excessive columns
    
    const crateName = path.basename(cratePath);

    for (let tableName in data.tables) {
      const table = data.tables[tableName];
      
      // Build headers with duplicates for array flattening, warn if > 5
      const headers = [];
      for (let prop in table.keys) {
        const count = table.keys[prop];
        if (count > 5) {
          console.warn(
            `Warning: Property '${prop}' occurs ${count} times, skipping in CSV output`,
          );
        } else {
          const hasRefs = table.refs[prop];
          for (let i = 0; i < count; i++) {
            if (hasRefs) {
              // Create paired columns for properties with references
              headers.push(prop);
              headers.push(`${prop}_id`);
            } else {
              // Create single repeated columns for simple properties
              headers.push(prop);
            }
          }
        }
      }

      // Build CSV rows by flattening array values across repeated columns
      const csvRows = [];
      for (let row of table.rows) {
        const csvRow = [];
        const headerIndex = {}; // Track which index we're at for each header (for the base prop)

        for (let header of headers) {
          // Check if this is an _id header
          if (header.endsWith("_id")) {
            const baseProp = header.slice(0, -3);
            if (!headerIndex[baseProp]) {
              headerIndex[baseProp] = 0;
            }
            const values = row[baseProp] || [];
            const value = values[headerIndex[baseProp]];
            csvRow.push(cleanTextForCSV(value?.id || ""));
            headerIndex[baseProp]++;
          } else {
            if (!headerIndex[header]) {
              headerIndex[header] = 0;
            }
            const values = row[header] || [];
            const value = values[headerIndex[header]];
            // If it's an object with name/id, extract name; otherwise use the value directly
            csvRow.push(cleanTextForCSV(value?.name || value || ""));
            // Only increment if the next header is not the _id for this same prop
            const nextHeaderIndex = headers.indexOf(header) + 1;
            if (
              nextHeaderIndex >= headers.length ||
              headers[nextHeaderIndex] !== `${header}_id`
            ) {
              headerIndex[header]++;
            }
          }
        }
        csvRows.push(csvRow);
      }

      // Generate CSV output
      const csv = stringify([headers, ...csvRows]);
      const csvFilename = `${crateName}_${tableName}.csv`;
      fs.writeFileSync(csvFilename, csv);
      console.log(`CSV saved to ${csvFilename}`);
    }
  });

program.parse(process.argv);
