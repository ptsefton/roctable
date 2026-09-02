#!/usr/bin/env node
import fs from "fs";
import path from "path";
import { Command } from "commander";
import { loadCrate } from "../lib/crate.js";
import { defaultConfig, loadConfig, saveConfig } from "../lib/config.js";
import { inspectCrate, mergeDiscovered, discoverExpandedProperties } from "../lib/inspect.js";
import { extractTables } from "../lib/extract.js";
import { tablesToCsvStrings, writeCsvFiles } from "../lib/csv.js";

const program = new Command();
program
  .name("roctable")
  .description("Convert RO-Crates to and from tabular formats");

program
  .command("inspect")
  .description(
    "Inspect a crate and write/update a config listing potential tables and properties",
  )
  .argument("<crate-dir>", "Path to the crate directory")
  .option("-c, --config <configPath>", "Path to the config file", "roctable-config.json")
  .action((crateDir, options) => {
    try {
      const { crate } = loadCrate(crateDir);
      const discovered = inspectCrate(crate);
      const existing = fs.existsSync(options.config) ? loadConfig(options.config) : defaultConfig();
      const merged = discoverExpandedProperties(crate, mergeDiscovered(existing, discovered));
      saveConfig(options.config, merged);
      console.log(`Config written to ${options.config}`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exitCode = 1;
    }
  });

program
  .command("csv")
  .description("Export a crate to CSV using a config produced by 'inspect'")
  .argument("<crate-dir>", "Path to the crate directory")
  .option("-c, --config <configPath>", "Path to the config file", "roctable-config.json")
  .option("-o, --output <outputDir>", "Output directory for CSV files", ".")
  .action((crateDir, options) => {
    try {
      if (!fs.existsSync(options.config)) {
        throw new Error(`config file not found: ${options.config}. Run 'roctable inspect' first.`);
      }
      const { crate, crateDir: resolvedDir } = loadCrate(crateDir);
      const config = loadConfig(options.config);
      const data = extractTables(crate, config, { crateDir: resolvedDir });
      const strings = tablesToCsvStrings(data);
      const crateName = path.basename(path.resolve(crateDir));
      const written = writeCsvFiles(strings, options.output, crateName);
      for (const file of written) console.log(`CSV saved to ${file}`);
    } catch (err) {
      console.error(`Error: ${err.message}`);
      process.exitCode = 1;
    }
  });

program.parse(process.argv);
