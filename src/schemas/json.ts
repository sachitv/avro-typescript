/** @internal */
export function _safeJSONStringify(obj: unknown, indent = 2): string {
  const cache: unknown[] = [];
  const retVal = JSON.stringify(obj, (_key, value) => {
    if (typeof value === "bigint") return String(value);
    if (typeof value === "object" && value !== null) {
      if (cache.includes(value)) return "[Circular]";
      cache.push(value);
    }
    return value;
  }, indent);
  cache.length = 0;
  return retVal;
}

export function safeStringify(value: unknown): string {
  // Converts supported primitive values and complex objects into stable JSON
  // while handling circular references and bigint values.
  if (
    typeof value === "string" || typeof value === "bigint" ||
    typeof value === "number" || typeof value === "boolean" || value === null ||
    typeof value === "undefined"
  ) {
    return String(value);
  }
  const jsonStr = _safeJSONStringify(value);
  if (jsonStr === undefined) {
    return String(value);
  }
  return `\n${jsonStr}\n`;
}
