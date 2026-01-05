#!/bin/sh

# Pre-commit hook to run deno fmt and deno lint
# Only formats and checks files that are staged for commit

# Get the repo root directory
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT" || exit 1

FMT_TMP=$(mktemp)
LINT_TMP=$(mktemp)
trap 'rm -f "$FMT_TMP" "$LINT_TMP"' EXIT

STAGED_FILES=$(git diff --cached --name-only --diff-filter=ACM)

while IFS= read -r file; do
    case "$file" in
        *.ts|*.tsx|*.js|*.jsx|*.mjs|*.mts)
            printf '%s\n' "$file" >> "$FMT_TMP"
            printf '%s\n' "$file" >> "$LINT_TMP"
            ;;
        *.json|*.jsonc|*.md)
            printf '%s\n' "$file" >> "$FMT_TMP"
            ;;
    esac
done <<EOF
$STAGED_FILES
EOF

if [ ! -s "$FMT_TMP" ] && [ ! -s "$LINT_TMP" ]; then
    echo "No files requiring fmt or lint staged for commit."
    exit 0
fi

if [ -s "$FMT_TMP" ]; then
    echo "Running deno fmt on staged files..."
    tr '\n' '\0' < "$FMT_TMP" | xargs -0 -r deno fmt
    tr '\n' '\0' < "$FMT_TMP" | xargs -0 -r git add --
fi

if [ -s "$LINT_TMP" ]; then
    echo "Running deno lint on staged files..."
    tr '\n' '\0' < "$LINT_TMP" | xargs -0 -r deno lint || {
        echo "deno lint failed. Aborting commit."
        exit 1
    }
fi

echo "Pre-commit checks passed."
