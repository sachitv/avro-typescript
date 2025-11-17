import { parse } from "jsonc";

const config = parse(await Deno.readTextFile("./deno.jsonc"));
console.log(config.version);