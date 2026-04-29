# OpenPlant Go SDK

This is the new OpenPlant V5 SDK module. It is intentionally separate from the
legacy `github.com/tc252617228/op` module in `../op`.

Current scope:

- typed domain models for OpenPlant V5 identifiers, values, status words, and
  time-series queries, including typed DS/LC helpers, alarm priority, point
  configuration fields, and point security groups
- immutable client options and service facades
- metadata reads for product/root/server/database, nodes, DAS, devices,
  replication, users, groups, access, lightweight points, and full point
  configuration over bounded readonly SQL, including calculation-point discovery
  through `PT=CALC` and non-empty `EX`
- explicit single-path query APIs for SQL, request/select, and native protocol
- realtime reads over native ID protocol, plus explicit SQL/request reads for
  ID or GN selectors
- archive/stat native reads through `QueryNative` without SQL/request fallback
- true streaming native archive/stat reads through `StreamNative`
- explicit OPConsole-style archive snapshot SQL reads through
  `Archive().SnapshotSQL`, projecting `RT` and `FM` for typed display workflows
- controlled mutation APIs for realtime native writes, archive native
  write/delete, and structured metadata/config table mutations
- explicit typed SQL row scanning with cached struct field plans
- bounded metadata point cache with configurable TTL and entry limit
- subscription stream lifecycle base with explicit source injection; GN drift
  rebinding is available only through the explicit `subscription.GNDriftSource`
  upper-layer wrapper
- realtime subscription over the request/select subscribe protocol for explicit ID
  subscription requests, including jittered same-protocol reconnect and
  resubscribe
- raw table subscription over the request/select subscribe protocol for
  explicit DB/table/key/index requests; Point and Alarm snapshot payloads have
  been verified against a real V5 target
- subscription events expose explicit kinds for data, terminal errors,
  reconnecting, and reconnected status changes
- active and historical alarm read APIs over bounded readonly SQL, including
  OPConsole-compatible projected alarm metadata such as `PN`, `AN`, `ED`,
  `EU`, `AP`, `LC`, and `C1..C8` when the server returns them
- OPConsole-style system metric helpers over explicit readonly SQL for
  `DB.SYS.*` calculation points such as session count, load, cache queue, event
  rate, and calculation time
- OPConsole-style `DB.SYS.*` point templates for admin tools; templates can be
  converted to `model.PointConfig` but are not written by the SDK
- explicit `admin.TableMutation` builders for system point templates; callers
  still opt in by invoking `Admin().MutateTable`
- typed `admin.TableMutation` builders for Node and Point configuration rows,
  including ID-bounded delete builders; they validate stable documented fields
  and still only construct requests
- typed `admin.TableMutation` builders for Replicator, RepItem, User, Groups,
  and Access configuration rows, including explicit user credential input
- calculation function catalog metadata for OpenPlant Lua formulas; the SDK
  indexes documented `op.*`, mirror-monitoring, and water/steam function
  families, extracts quoted point references, and lint-checks obvious formula
  issues without executing Lua
- calculation dependency graph helpers for `PointConfig.EX`, including
  dependency-first order, cycle detection, and `KO` order diagnostics
- typed replication/mirror enums and validation helpers for `SY`, `XF`, `FL`,
  `AR`, and `TL` semantics
- mirror diagnostics for Replicator/RepItem configuration and
  `op.ar_sync_time` / `op.rt_sync_time` monitoring formulas
- SQL `LIKE` helper functions that escape wildcard characters and emit the
  OpenPlant-supported `ESCAPE '\'` clause
- safe-readonly integration coverage for metadata, realtime, archive, stat,
  native archive/stat, and alarm read paths
- read-only SQL safety boundary
- standard OpenPlant server error-code lookup metadata derived from the
  official OPConsole locale files, including reconnect classification for
  network/transport codes
- readonly integration-test environment scaffolding

The SDK defaults to readonly mode. Passwords and real database credentials must
come from environment variables or process memory and must not be committed.

Default query methods do not perform hidden fallback. `QuerySQL` only uses SQL,
`QueryRequest` only uses request/select, and native APIs only use the native
protocol. The low-level SDK does not provide cross-path fallback helpers;
callers that want orchestration must build it above the SDK with visible
policy, tracing, and cost control. `examples/strategy` shows that pattern
without adding it to the base SDK.

Native paths require point IDs. If callers want to use GNs, they should resolve
them explicitly through `Metadata()` or use a SQL/request path that natively
accepts GN selectors. The SDK does not do hidden metadata SQL inside native
APIs, and native realtime results do not populate `Sample.GN` from the metadata
cache.

`SQL().Query` is intentionally conservative: it accepts only `SELECT` and
readonly `WITH ... SELECT` statements. Use `SQL().ExecUnsafe` with explicit
write opt-in for every mutation or nonstandard administrative statement.

Subscription streams follow the same rule: data, errors, and reconnect status
are visible as `Event.Kind` values. The SDK does not hide reconnect behavior
behind data-only events.

## Safety Defaults

```powershell
$env:OPENPLANT_TEST_HOST="<host>"
$env:OPENPLANT_TEST_PORT="8200"
$env:OPENPLANT_TEST_USER="<user>"
$env:OPENPLANT_TEST_PASS="..."
$env:OPENPLANT_TEST_READONLY="1"
$env:OPENPLANT_TEST_DB="W3"              # optional, otherwise discovered
$env:OPENPLANT_TEST_POINT_ID="1001"      # optional
$env:OPENPLANT_TEST_POINT_GN="W3.N.P1"   # optional alternative
```

Keep real connection values in your local shell environment or an ignored local
env file. Do not commit hostnames, usernames, passwords, or production point
names.

Mutation tests are opt-in only. They may write to a real database only inside a
fresh isolated namespace: the lifecycle test creates its own Node and Point,
writes realtime/archive values, reads them back, updates the new point config,
checks realtime subscription for that point, then deletes the archive samples,
Point, and Node it created.

Run the default unit suite:

```powershell
go test ./...
```

Run real-database safe readonly checks only when the environment above is set:

```powershell
go test -tags safe_readonly ./...
```

Some optional configuration/request resources can return server
`resource not available` on a given V5 deployment. The safe-readonly suite
treats those as target capability skips after the core metadata, realtime,
archive/stat, and alarm paths have been exercised.

## Known Boundaries

The root `docs` interface manuals describe broader legacy surfaces such as
generic `find/insert/update/remove`, native-by-name calls, and Realtime/Alarm/
Point subscription variants. The base SDK does not hide those behind existing
methods.

- Native base APIs stay ID-only. Use explicit metadata lookup first, or use
  SQL/request paths with GN selectors, such as `Realtime().QuerySQL` or
  `Realtime().QueryRequest`.
- The default `Subscribe` path remains realtime ID subscription over
  request/select subscribe. GN drift rebinding is an explicit
  `subscription.GNDriftSource` wrapper using
  `client.RealtimeSubscriptionIDSource()` as its ID source.
- `Subscription().SubscribeTable` is an explicit raw-row table subscription
  path for verified table payloads such as `Point` and `Alarm`. It requires the
  caller to provide DB, table, key column, and index values; it does not resolve
  GNs or convert rows into typed domain records. `examples/subscription` shows
  thin application-level typed Point/Alarm wrappers over this raw stream.
- Generic table mutation is guarded under `Admin().MutateTable` and refuses
  time-series tables. Realtime/archive writes use their dedicated native APIs.
- Calculation formula authoring and mirror/config write helpers are not broad
  convenience APIs yet; the SDK currently exposes formula/function catalogs,
  reference extraction, dependency diagnostics, system point templates, explicit
  mutation builders, typed Node/Point/mirror/security builders, and typed
  replication diagnostics. Actual config changes still require
  `Admin().MutateTable` and writable client options.

Run mutation checks only against an isolated namespace:

```powershell
$env:OPENPLANT_TEST_MUTATION="1"
$env:OPENPLANT_TEST_READONLY="0"
$env:OPENPLANT_TEST_DB="W3"
$env:OPENPLANT_TEST_PREFIX="SDK_MUTATION_TEST_"
go test -tags mutation ./...
```

Do not point mutation tests at existing business objects. The tests require
`OPENPLANT_TEST_DB` and `OPENPLANT_TEST_PREFIX` so generated names are bounded
and cleanup can target the created IDs.

Write surfaces are deliberately explicit:

- `Realtime().WriteNative(ctx, RealtimeWriteRequest{...})`
- `Archive().WriteNative(ctx, ArchiveWriteRequest{...})`
- `Archive().DeleteNative(ctx, ArchiveDeleteRequest{...})`
- `Admin().MutateTable(ctx, TableMutation{...})`

All of them are blocked when `ReadOnly` is true. Generic table mutation rejects
time-series tables; use the dedicated realtime/archive APIs for those paths.

## Performance Surfaces

Native archive/stat streaming avoids materializing the whole response before
calling user code:

```go
err := client.Archive().StreamNative(ctx, query, func(sample openplant.Sample) bool {
	return true
})
```

`QueryNative` is still available when the caller wants a slice result; it is
implemented on top of the streaming decoder.

SQL row maps can be converted explicitly with cached struct metadata:

```go
import openplantsql "github.com/tc252617228/openplant/sql"

type PointRow struct {
	ID int32  `openplant:"ID"`
	GN string `openplant:"GN"`
}

rows, err := openplantsql.ScanRows[PointRow](result.Rows)
```

Metadata point cache sizing is controlled with
`WithMetadataCacheTTL` and `WithMetadataCacheMaxEntries`.

The benchmark suite covers native archive/stat decode and streaming, native
request encoding, realtime/archive write encoding, subscription response
decode, table-select encoding, archive/stat SQL row conversion, dataset decode,
typed SQL scan, point cache lookup/store, and connection pool acquire/release.

Run all benchmarks with:

```powershell
go test -bench . ./...
```

Run a focused benchmark subset with:

```powershell
go test -run '^$' -bench 'Benchmark(StreamNative|DecodeNative|EncodeNative|EncodeRealtime|EncodeArchive|TSValue|DecodeDataSet|RowDecoder|PointCache|ScanRows|PoolAcquireRelease|TableSelect|Subscription|ArchiveQuerySQLRows|StatQuerySQLRows)' -benchtime=100x ./...
```

Run opt-in soak tests with an explicit duration:

```powershell
$env:OPENPLANT_TEST_SOAK="1"
$env:OPENPLANT_TEST_SOAK_DURATION_MS="3000"
go test -tags soak ./subscription ./internal/transport -count=1
go test -tags 'safe_readonly soak' ./tests -run TestSafeReadonlySoak -v -count=1
```

## Documentation Status

The V5 contract extracted from the provided PDFs is stored in
`docs-contract.md`. It covers table shapes, DS/LC/AP/SG semantics, SQL modes,
safe query constraints, calculation functions, replication/configuration
fields, OPConsole-derived system metrics, and SDK safety rules. Treat it as the
local source of truth until a behavior is verified against a real V5 server.

The repository-root `docs` directory is the primary source for product and API
behavior. Check it before changing protocol, table, SQL, realtime, archive,
stat, alarm, subscription, mirror, or admin behavior.

## Packages

- `model`: core typed IDs, GN, RT/PT, DS, LC, samples, ranges, modes
- `sql`: read-only SQL validation, explicit unsafe execution boundary, and
  typed row scanning helpers
- `metadata`, `realtime`, `archive`, `stat`, `alarm`, `subscription`, `admin`:
  domain service boundaries
- `system`: OPConsole-style `DB.SYS.*` readonly metrics and point templates
- `calc`: calculation formula function catalog, reference extraction, and
  lightweight linting/dependency diagnostics; no Lua execution
- `mirror`: local mirror configuration diagnostics and sync-monitor discovery
- `operror`: SDK error classification

See `docs-contract.md` for the V5 contract extracted from the provided PDFs.

## More

- [Examples](examples/README.md)
- [Migration notes](MIGRATION.md)
- [Benchmark notes](benchmarks/README.md)
- [AI skill](skill/openplant-sdk/SKILL.md)
