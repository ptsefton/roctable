// Turn a discovered { properties: { prop: { include: false } } } map from
// inspectCrate into a table's `properties` config with everything selected,
// for integration tests that want "export every column" without hand-listing
// each property. `overrides` lets a test tweak individual properties (e.g.
// turn on `expand`, or exclude one) on top of the select-all default.
export function selectAll(propertiesMap, overrides = {}) {
  const properties = {};
  for (const prop of Object.keys(propertiesMap)) {
    properties[prop] = { include: true, ...(overrides[prop] || {}) };
  }
  return properties;
}
