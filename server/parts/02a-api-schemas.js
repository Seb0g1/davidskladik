// Lightweight shape-validation for marketplace API responses.
// Logs a warning when an expected field path is missing — helps detect API changes
// before they silently corrupt data. Never throws; always returns a result object.

function getNestedValue(obj, path) {
  // Supports dot-notation and [N] array indexing: "result.items[0].offer_id"
  const parts = path.replace(/\[(\d+)\]/g, ".$1").split(".");
  let current = obj;
  for (const part of parts) {
    if (current == null || typeof current !== "object") return undefined;
    current = current[part];
  }
  return current;
}

function validateApiShape(response, expectedPaths, context) {
  const missingPaths = [];
  const wrongTypes = [];

  for (const pathSpec of expectedPaths) {
    // Support optional type hint: "result.items[0].stock:number"
    const [path, expectedType] = pathSpec.split(":");
    const value = getNestedValue(response, path);

    if (value === undefined || value === null) {
      missingPaths.push(path);
    } else if (expectedType && typeof value !== expectedType) {
      wrongTypes.push({ path, expected: expectedType, got: typeof value });
    }
  }

  const valid = missingPaths.length === 0 && wrongTypes.length === 0;

  if (!valid) {
    logger.warn("api_shape_mismatch", {
      context,
      missingPaths: missingPaths.length ? missingPaths : undefined,
      wrongTypes: wrongTypes.length ? wrongTypes : undefined,
    });
  }

  return { valid, missingPaths, wrongTypes };
}
