import { build, emptyDir } from "@deno/dnt";
import { parse } from "@std/jsonc";

const denoConfigText = await Deno.readTextFile("./deno.jsonc");
const denoConfig = parse(denoConfigText) as { version?: string };
const pkgVersion = typeof denoConfig.version === "string"
  ? denoConfig.version
  : "0.0.0";

// Produce the npm package artifacts from the canonical Deno entrypoint.
await emptyDir("./npm");

await build({
  typeCheck: false,
  test: false,
  scriptModule: false, 
  entryPoints: ["./src/mod.ts"],
   outDir: "./npm",
   package: {
     name: "@sachitv/avro-typescript",
     version: pkgVersion,
     description: "Avro schema parsing, serialization, and RPC helpers for TypeScript runtimes.",
     license: "MIT",
     repository: {
       type: "git",
       url: "https://github.com/sachitv/avro-typescript",
     },
     bugs: {
       url: "https://github.com/sachitv/avro-typescript/issues",
     },
     homepage: "https://github.com/sachitv/avro-typescript",
     engines: {
       node: ">=22",
     },
     sideEffects: false,
     publishConfig: {
       access: "public",
     },
     type: "module",
     exports: {
       ".": {
         import: "./esm/mod.mjs",
         types: "./esm/mod.d.ts",
       },
     },
   },
   packageManager: "npm",
   shims: {
     deno: "dev",
   },
   compilerOptions: {
     lib: ["ESNext", "DOM"],
   },
 });
