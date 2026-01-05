#!/bin/sh

# Pre-commit hook to run deno fmt and deno lint
# Only formats and checks files that are staged for commit

# Get the repo root directory
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT" || exit 1

STAGED_FILES=$(git diff --cached --name-only --diff-filter=ACM)

FMT_FILES=$(echo "$STAGED_FILES" | grep -E '\.(ts|tsx|js|jsx|mjs|mts|json|jsonc|md)$')
LINT_FILES=$(echo "$STAGED_FILES" | grep -E '\.(ts|tsx|js|jsx|mjs|mts)$')

if [ -z "$FMT_FILES" ] && [ -z "$LINT_FILES" ]; then
    echo "No files requiring fmt or lint staged for commit."
    exit 0
fi

if [ -n "$FMT_FILES" ]; then
    echo "Running deno fmt on staged files..."
    printf '%s\n' "$FMT_FILES" | tr '\n' '\0' | xargs -0 -r deno fmt

    printf '%s\n' "$FMT_FILES" | while IFS= read -r file; do
        if [ -n "$file" ] && [ -n "$(git diff --name-only -- "$file")" ]; then
            git add "$file"
        fi
    done
fi

if [ -n "$LINT_FILES" ]; then
    echo "Running deno lint on staged files..."
    printf '%s\n' "$LINT_FILES" | tr '\n' '\0' | xargs -0 -r deno lint || {
        echo "deno lint failed. Aborting commit."
        exit 1
    }
fi

echo "Pre-commit checks passed."
