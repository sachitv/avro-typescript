# GitHub Copilot Instructions for avro-typescript

This document provides instructions for GitHub Copilot when working on the avro-typescript repository.

## Project Overview

avro-typescript is an open source implementation of Apache Avro for TypeScript that mirrors the Avro specifications from Apache Avro. It provides tooling around schema parsing, serialization, and code generation with zero runtime dependencies. The project leverages modern Web APIs and is designed to work across multiple JavaScript runtimes.

## Agent Types

### Coding Agent
When writing or modifying code:
- Ensure all tests pass before committing changes
- Maintain 100% code coverage for lines, branches, and functions
- Format all files with `deno fmt` before committing
- Use conventional commit format: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`
- Include detailed commit messages explaining what and why changes were made

### Review Agent
When reviewing code changes:
- Verify that all tests pass
- Check that code coverage remains at 100%
- Ensure code is properly formatted with `deno fmt`
- Confirm conventional commit format is used
- Validate that changes align with project goals and Avro specifications

## Repository Guidelines

### Project Structure
- `src/` contains runtime exports, schema definitions, serialization helpers, and logical-type adapters
- `examples/` hosts runnable demos
- `test-data/` stores sample Avro fixtures
- `scripts/` keeps tooling helpers
- `packages/` contains benchmarks and related packages

### Coding Style & Naming Conventions
- Use Deno's formatter and linter (no custom config)
- Two-space indentation with trailing commas
- Module files use lowercase snake_case: `avro_reader.ts`, `logical_type.ts`
- Export PascalCase types or camelCase helpers
- Sync APIs must be 1:1 ports of async counterparts

### Testing Guidelines
- Tests run with `deno task test` using fixtures from `test-data/`
- Coverage must be 100% for lines, branches, and functions
- Add samples to `test-data/` when creating new schemas or serialized blobs

### Commit & Pull Request Guidelines
- Use conventional commit format: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`
- Include detailed commit messages with context
- PR descriptions must include summary, changes, context/motivation, breaking changes, tests, and migration notes
- Run fmt, lint, coverage checks before submitting

## Task Management

Use structured task management for complex operations:
1. Break down complex tasks into specific, actionable items
2. Mark tasks as in_progress when working on them
3. Complete tasks immediately after finishing
4. Only work on one task at a time

## Professional Guidelines

- Prioritize technical accuracy and truthfulness
- Provide direct, objective technical information
- Focus on facts and problem-solving
- Use GitHub-flavored markdown for formatting
- Output concise responses in monospace font
- Reference specific code locations with `file_path:line_number`

## Working with Avro Types

When assisting with Avro schema work:
- Use `createType` factory for schema construction
- Support primitive names, full schema objects, unions, and named types
- Handle namespace resolution and registry tracking
- Work with AvroReader and AvroWriter for file operations
- Implement custom readable/writable buffers when needed

## RPC Protocol Support

For RPC-related tasks:
- Follow Avro protocol wire format specifications
- Use handshake helpers for protocol negotiation
- Implement call request/response encoding/decoding
- Apply message framing for transport
- Work with EventTarget-based Protocol API for service implementation

## Common Commands

```bash
# Development workflow
deno fmt                    # Format code (all files)
deno fmt file.ts           # Format specific file
deno lint                   # Run linter (all files)
deno lint file.ts          # Lint specific file
deno task test             # Execute tests (all files)
deno task test file.test.ts  # Test specific file
deno task test:ci          # CI test run with coverage
deno task coverage         # Generate coverage data (all files)
deno task coverage file.test.ts  # Generate coverage data for specific file
deno task coverage:check   # Verify 100% coverage
deno task serve:coverage   # View coverage report
deno task get-version      # Get package version

# Installation
deno add jsr:@sachitv/avro-typescript
```

## Getting Help

- Review DEVELOP.md for detailed development setup
- Check example repositories for runtime-specific implementations

Remember to always follow the repository conventions and ensure code quality through proper testing, formatting, and coverage requirements.