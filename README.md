# Avro TypeScript

![CI](https://github.com/sachitv/avro-typescript/actions/workflows/ci.yml/badge.svg?branch=main)

`avro-typescript` is an open source implementation of Apache Avro for
TypeScript. It mirrors the Avro specifications from
[Apache Avro](https://avro.apache.org/) and provides tooling around schema
parsing, serialization, and code generation. The project aims to leverage modern
Web APIs with zero runtime dependencies, making it easy to integrate across
multiple JavaScript runtimes. See the [docs](./docs/README.md) for usage
guidance, design details, and examples.

## Goals

- Align with the [Avro specification](https://avro.apache.org/) via schema
  parsing, serialization, and file support.
- Offer support for files written in the Avro container format for storage and
  streaming scenarios.
- Explore the Avro protocol specification with experimental tooling for
  RPC-style definitions to ease adoption in service ecosystems.

This package relies on the Deno toolchain for development workflows. Install
Deno by following the instructions at
https://deno.com/manual/getting_started/installation.

## Development

Contributor tooling, dependency details, and VS Code devcontainer instructions
live in [DEVELOP.md](./DEVELOP.md). Follow that guide for formatting, linting,
testing, and coverage commands defined in `deno.jsonc`.
