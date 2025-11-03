# TypeScript Module Tooling

This package relies on the Deno toolchain for development workflows. Install
Deno by following the instructions at
https://deno.com/manual/getting_started/installation.

## Testing

Run the suite with `deno test` from the `lang/ts` directory. The command
respects the configuration in `deno.json` and requires no setup beyond having
Deno installed.

## Coverage

Generate coverage after running the tests with `--coverage`. The report uses
data produced by `deno test` and writes the summarized output into the
`coverage` directory. View the coverage only in the CLI using `deno coverage`.
