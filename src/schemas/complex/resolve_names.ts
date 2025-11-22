const NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Checks if the given name is a valid Avro identifier.
 * @param name - The name to validate.
 * @returns True if the name is valid, false otherwise.
 */
export function isValidName(name: string): boolean {
  return NAME_PATTERN.test(name);
}

const PRIMITIVE_TYPE_NAMES = new Set([
  "null",
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "bytes",
  "string",
]);

/**
 * Parameters for resolving Avro names, including namespace qualification and aliases.
 */
export interface ResolveNamesParams {
  /** The base name of the type. */
  name: string;
  /** Optional namespace to qualify the name. */
  namespace?: string;
  /** Optional list of aliases for the type. */
  aliases?: string[];
}

/**
 * Represents the resolved names for an Avro type, including its full name, aliases, and namespace.
 */
export interface ResolvedNames {
  /** The fully qualified name of the type. */
  fullName: string;
  /** The resolved aliases for the type. */
  aliases: string[];
  /** The namespace of the type. */
  namespace: string;
}

/**
 * Resolves the full name, aliases, and namespace for an Avro type based on the provided parameters.
 */
export function resolveNames(
  { name, namespace, aliases = [] }: ResolveNamesParams,
): ResolvedNames {
  const fullName = qualifyName(name, namespace);
  const typeNamespace = getNamespaceFromName(fullName);
  const resolvedAliases: string[] = [];
  const seen = new Set<string>();
  seen.add(fullName);

  for (const alias of aliases) {
    const qualified = qualifyAlias(alias, typeNamespace);
    if (!seen.has(qualified)) {
      seen.add(qualified);
      resolvedAliases.push(qualified);
    }
  }

  return {
    fullName,
    aliases: resolvedAliases,
    namespace: typeNamespace ?? "",
  };
}

function qualifyName(
  name: string,
  namespace?: string,
): string {
  if (!name) {
    throw new Error("Avro name is required.");
  }

  let qualified = name;
  if (namespace && !name.includes(".")) {
    qualified = `${namespace}.${name}`;
  }

  validateFullName(qualified, false);
  return qualified;
}

function qualifyAlias(alias: string, namespace?: string): string {
  if (!alias) {
    throw new Error("Avro alias is required.");
  }

  let qualified = alias;
  if (namespace && !alias.includes(".")) {
    qualified = `${namespace}.${alias}`;
  }

  validateFullName(qualified, true);
  return qualified;
}

function validateFullName(
  name: string,
  isAlias: boolean,
): void {
  const parts = name.split(".");
  parts.forEach((part) => {
    if (!isValidName(part)) {
      throw new Error(
        `${isAlias ? "Invalid Avro alias: " : "Invalid Avro name: "}${name}`,
      );
    }
  });

  const tail = parts[parts.length - 1];
  if (PRIMITIVE_TYPE_NAMES.has(tail)) {
    throw new Error(
      `${
        isAlias
          ? "Cannot rename primitive Avro alias: "
          : "Cannot rename primitive Avro type: "
      }${tail}`,
    );
  }
}

function getNamespaceFromName(name: string): string | undefined {
  const parts = name.split(".");
  if (parts.length <= 1) {
    return undefined;
  }
  parts.pop();
  return parts.join(".");
}
