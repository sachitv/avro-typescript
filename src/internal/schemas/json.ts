export function safeJsonStringify(obj: unknown, indent = 2): string {
  const cache: unknown[] = [];
  const retVal = JSON.stringify(obj, (_key, value) => {
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
  if (typeof value === "string") return value;
  try {
    const jsonStr = safeJsonStringify(value);
    if (jsonStr === undefined) {
      return String(value);
    }
    return `\n${jsonStr}\n`;
  } catch {
    return String(value);
  }
}
