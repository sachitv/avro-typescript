# Repository Guidelines

-## Project Structure & Module Organization

- `src/` contains the runtime exports, schema definitions, serialization
  helpers, and logical-type adapters that power the package; follow the existing
  module layout when adding new symbols (see `src/mod.ts:1-40` for current
  naming patterns).
- `examples/` hosts runnable demos, `test-data/` stores sample Avro fixtures for
  the test suite, and `scripts/` keeps tooling such as `pre-commit.sh`, so place
  helpers or fixtures near the task they support.
- Documentation and onboarding (README, DEVELOP) describe the Deno-first
  workflow, devcontainer choices, and hook setup; keep those references in sync
  when workflows change (`DEVELOP.md:1-42`).
- The `coverage/` directory is populated by coverage tasks; treat it as derived
  output that can be regenerated from the task suite defined in
  `deno.jsonc:15-24`.

## Build, Test, and Development Commands

- `deno fmt` – applies the official formatter to every tracked TypeScript file
  prior to a commit or PR (`DEVELOP.md:19-28`).
- `deno lint` – runs the default lint suite configured in `deno.jsonc`, catching
  stylistic and static-analysis issues before pushing (`DEVELOP.md:19-28`).
- `deno task test` – executes the project tests with `--allow-read`; it is the
  baseline for any change (`deno.jsonc:15-24`).
- `deno task test:ci` – replicates CI behavior by adding JUnit output and
  coverage flags alongside the normal test run (`deno.jsonc:15-24`).
- `deno task coverage` + `deno task coverage:check` – collect coverage data and
  enforce the 100% thresholds defined in `.denocoveragerc:1-7`; run both before
  submitting changes.
- `deno task serve:coverage` – serves the generated HTML report for local
  inspection (`deno.jsonc:15-24`).
- `deno task get-version` – shows the current package version via
  `scripts/get_version.ts` for release or documentation updates
  (`deno.jsonc:15-24`).
- `scripts/pre-commit.sh` (symlinked per `DEVELOP.md:30-42`) wraps fmt+lint to
  keep commits clean.

## Coding Style & Naming Conventions

- The codebase relies on Deno’s formatter and linter rather than a custom
  config, so run `deno fmt`/`deno lint` frequently; the TypeScript files follow
  two-space indentation with trailing commas and consistent export blocks
  (`src/mod.ts:1-40`).
- Module files use lowercase snake_case filenames (e.g., `avro_reader.ts`,
  `logical_type.ts`) and export PascalCase types or camelCase helpers, mirroring
  the existing schema/serialization directories (`src/mod.ts:1-40`).
- Keep documentation and examples aligned with the runtime structure so
  consumers can trace primitives, complexes, and logical types from namespaced
  exports to file paths.

## Testing Guidelines

- Tests run on Deno’s built-in harness (`deno task test`) with fixtures stored
  under `test-data/`; add samples there when you need new schemas or serialized
  blobs.
- Coverage is guarded by `.denocoveragerc`, which demands 100% for lines,
  branches, and functions on every file, so every test change must be
  accompanied by coverage that satisfies `deno task coverage:check` before the
  PR (`.denocoveragerc:1-7`).
- Use `deno task serve:coverage` to inspect failing areas interactively if a new
  test exercises complex serialization paths.

## Commit & Pull Request Guidelines

- Follow the conventional commit pattern (`type: description`, optionally
  scoped) demonstrated by recent history (e.g., `feat:`, `fix:`, `chore:`).
- Each PR needs a descriptive summary, list of tests/commands run (fmt, lint,
  coverage), and any linked issue numbers or design notes; mention migrations
  through `scripts/get_version.ts` if you bump package info.
- Since `scripts/pre-commit.sh` links into Git hooks (`DEVELOP.md:30-42`),
  ensure it is executable locally or manually run its pipeline before pushing so
  CI sees the same clean state as contributors.
