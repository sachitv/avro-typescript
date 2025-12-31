---
name: lint
description: Check for style and static analysis warnings using Deno lint
license: MIT
compatibility: opencode
metadata:
  workflow: deno
---

## What I do

- Run `deno lint` to check for style and static analysis warnings
- Can also lint a single file using `deno lint <filename>`
- Uses the default Deno lint rules configured in `deno.jsonc`
- Catches potential issues and maintains code quality standards

## When to use me

Use this when you want to verify code quality:

- Before committing changes
- Before creating a pull request
- After making significant code modifications
- As part of the CI/CD pipeline

## Notes

- Customization of lint rules can be done in `deno.jsonc` if necessary
- This is part of the standard pre-commit workflow defined in
  `scripts/pre-commit.sh`
