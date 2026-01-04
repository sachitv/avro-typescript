#!/bin/sh

# Pre-commit hook to run deno fmt and deno lint
# Only formats and checks files that are staged for commit

# Get the repo root directory
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT" || exit 1

# Get list of staged TypeScript files
STAGED_TS_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep -E '\.(ts|tsx)$')

if [ -z "$STAGED_TS_FILES" ]; then
    echo "No TypeScript files staged for commit."
    exit 0
fi

echo "Running deno fmt on staged files..."
# Format only the staged files
echo "$STAGED_TS_FILES" | xargs deno fmt

# Check if deno fmt made changes to staged files
CHANGED_STAGED_FILES=""
for file in $STAGED_TS_FILES; do
    if [ -n "$(git diff --name-only -- "$file")" ]; then
        CHANGED_STAGED_FILES="$CHANGED_STAGED_FILES $file"
    fi
done

if [ -n "$CHANGED_STAGED_FILES" ]; then
    echo "deno fmt made changes. Adding formatted files to staging..."
    echo "$CHANGED_STAGED_FILES" | xargs git add
fi

echo "Running deno lint..."
deno lint

if [ $? -ne 0 ]; then
    echo "deno lint failed. Aborting commit."
    exit 1
fi

echo "Pre-commit checks passed."
