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
    echo "$FMT_FILES" | xargs deno fmt

    CHANGED_STAGED_FILES=""
    for file in $FMT_FILES; do
        if [ -n "$(git diff --name-only -- "$file")" ]; then
            CHANGED_STAGED_FILES="$CHANGED_STAGED_FILES $file"
        fi
    done

    if [ -n "$CHANGED_STAGED_FILES" ]; then
        echo "deno fmt made changes. Adding formatted files to staging..."
        echo "$CHANGED_STAGED_FILES" | xargs git add
    fi
fi

if [ -n "$LINT_FILES" ]; then
    echo "Running deno lint on staged files..."
    echo "$LINT_FILES" | xargs deno lint

    if [ $? -ne 0 ]; then
        echo "deno lint failed. Aborting commit."
        exit 1
    fi
fi

echo "Pre-commit checks passed."
