#!/bin/sh

# Pre-commit hook to run deno fmt and deno lint on lang/ts

echo "Running deno fmt on lang/ts..."
cd lang/ts
deno fmt .

# Check if deno fmt made changes
if [ -n "$(git diff --name-only)" ]; then
    echo "deno fmt made changes. Adding them to staging..."
    git add -u
fi

echo "Running deno lint on lang/ts..."
deno lint

if [ $? -ne 0 ]; then
    echo "deno lint failed. Aborting commit."
    exit 1
fi

cd ..
echo "Pre-commit checks passed."