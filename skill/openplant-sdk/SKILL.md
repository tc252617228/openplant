---
name: openplant-sdk
description: Use when maintaining, extending, or reviewing the OpenPlant Go SDK, especially API boundaries, protocol behavior, examples, docs, tests, performance work, or changes that must preserve explicit SQL/request/native paths without hidden fallback.
---

# OpenPlant SDK

## Core Rules

Work in the `openplant` module. Use `docs-contract.md`, public package
documentation, examples, tests, and any available upstream OpenPlant product
manuals as behavior references before changing protocol, table, SQL, realtime,
archive, stat, alarm, subscription, mirror, or admin behavior.

Preserve the explicit API contract:

- `SQL().Query` only uses SQL.
- `QueryRequest` only uses request/select.
- `QueryNative` and `StreamNative` only use native protocol.
- Cross-path composition, retry layering, and native/request/SQL orchestration
  are not low-level SDK features. Keep them in application-owned code that
  exposes policy and trace.
- Do not add hidden fallback, silent degradation, or compatibility paths copied
  from legacy APIs.
- If a path is not supported by the target database, return that error
  transparently instead of compensating with another protocol, broad scan, extra
  connection, or hidden retry.

Default behavior must stay read-only. Writes require explicit write APIs and
`WithReadOnly(false)`.

## Workflow

1. Read the relevant package and contract documentation for the behavior being
   changed.
2. Search with `rg`; prefer existing package patterns over new abstractions.
3. Keep API surfaces typed, narrow, and domain-specific.
4. Add tests near the changed package. Add safe-readonly or mutation-tag tests
   only when behavior needs a real server boundary.
5. Update README, examples, migration notes, or benchmark notes when public
   behavior changes.
6. Run `gofmt` and the smallest useful validation command, then broaden before
   finishing broad SDK changes.

## Package Map

- `metadata`: bounded readonly metadata SQL APIs.
- `realtime`: realtime read and explicit native write.
- `archive`: SQL/request/native read, native stream, native write/delete.
- `stat`: SQL/request/native read and native stream.
- `alarm`: bounded readonly active and historical alarm reads.
- `subscription`: visible realtime ID event stream and explicit raw-row table
  subscriptions with reconnect status events.
- `system`: OPConsole-style `DB.SYS.*` metric helpers and point templates.
- `mirror`: local mirror diagnostics; do not query mirror state implicitly.
- `sql`: readonly SQL boundary and typed row scanner.
- `admin`: guarded generic config/metadata mutation and typed mutation
  builders. Callers must execute mutations explicitly with writable client
  options.
- `calc`: calculation function catalog, formula reference helpers, dependency
  graph helpers, and lightweight linting. Do not execute Lua in the SDK.
- `model`: typed RT/PT/DS/LC/AP/SG semantics and value wrappers.
- `operror`: OpenPlant server and transport error classification.

For detailed API and validation guidance, read
`references/openplant-sdk.md` when the task touches public APIs, docs, examples,
or tests.

## Validation

Use the smallest useful test first, then broaden before finishing:

```powershell
go test ./...
go test ./... -p=1 -count=1
go test -tags safe_readonly ./...
go test -tags mutation ./... -p=1 -count=1
```

Run benchmarks for performance work:

```powershell
go test -bench . ./...
```

Focused benchmark subset:

```powershell
go test -run '^$' -bench 'Benchmark(StreamNative|DecodeNative|EncodeNative|EncodeRealtime|EncodeArchive|TSValue|DecodeDataSet|RowDecoder|PointCache|ScanRows|PoolAcquireRelease|TableSelect|Subscription|ArchiveQuerySQLRows|StatQuerySQLRows)' -benchmem ./...
```

Opt-in soak tests:

```powershell
$env:OPENPLANT_TEST_SOAK="1"
$env:OPENPLANT_TEST_SOAK_DURATION_MS="3000"
go test -tags soak ./subscription ./internal/transport -count=1
go test -tags 'safe_readonly soak' ./tests -run TestSafeReadonlySoak -v -count=1
```
