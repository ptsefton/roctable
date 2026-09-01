import fs from "fs";

export const DEFAULT_MAX_REPEAT = 10;

export function defaultConfig() {
  return {
    defaults: { max_repeat: DEFAULT_MAX_REPEAT },
    tables: {},
    potential_tables: {},
  };
}

export function loadConfig(configPath) {
  return JSON.parse(fs.readFileSync(configPath, "utf8"));
}

export function saveConfig(configPath, config) {
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + "\n");
}

export function getMaxRepeat(config, tableName) {
  return (
    config.tables?.[tableName]?.max_repeat ??
    config.defaults?.max_repeat ??
    DEFAULT_MAX_REPEAT
  );
}
