# Development

## Requirements

- The only required tooling is
  [Deno](https://deno.com/manual/getting_started/installation), which covers
  formatting, linting, testing, and local execution.

## Devcontainer

- A `.devcontainer` folder is provided; see the overview at
  https://containers.dev/ for context on how devcontainers work.
- In VS Code, install the **Dev Containers** extension, open this repo, and use
  **Dev Containers: Reopen in Container** (or **Open Folder in Container**) from
  the command palette.
- Pick one of the available definitions (`deno-only` or `deno-and-ai`) to get a
  consistent environment with Deno ready to go.

## Workflows

- `deno fmt`: runs the formatter configured by Deno to keep the TypeScript
  sources consistent before committing or submitting a pull request.
- `deno lint`: checks for style and static analysis warnings using the default
  Deno lint rules (customize in `deno.jsonc` if necessary).
- `deno task test`: executes the test suite.
- `deno task coverage`: builds on the `test` task to emit coverage data to the
  `coverage` directory; serve the results with `deno task serve:coverage` if you
  want an HTML report that you can view in your browser.
