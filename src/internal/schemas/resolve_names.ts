const NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

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

export interface ResolveNamesParams {
  name: string;
  namespace?: string;
  aliases?: string[];
}

export interface ResolvedNames {
  fullName: string;
  aliases: string[];
  namespace: string;
}

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
