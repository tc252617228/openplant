# Migration Notes from `op` to `openplant`

`openplant` is not a drop-in wrapper around the legacy `op` module. It keeps the
useful protocol knowledge and changes the SDK contract to favor predictable,
single-purpose APIs.

## Core Rule

Default APIs do not perform hidden fallback.

- `SQL().Query` only uses SQL.
- `Realtime().Read` only uses the native ID path; `Realtime().QuerySQL` and
  `Realtime().QueryRequest` are explicit non-native alternatives.
- `Archive().QueryRequest` and `Stat().QueryRequest` only use request/select.
- `Archive().QueryNative`, `Archive().StreamNative`, `Stat().QueryNative`, and
  `Stat().StreamNative` only use native protocol.
- Multi-path behavior is not part of the low-level SDK. If an application wants
  native/request/SQL orchestration, it must own that policy above the SDK and
  expose the trace/cost to callers. See `examples/strategy` for an
  application-owned policy/trace helper.
- Native paths require point IDs and do not resolve GNs through hidden metadata
  SQL or cache state. Resolve GNs explicitly with `Metadata().FindPoints` or
  use SQL/request paths that accept GN selectors.

This is intentional. The SDK must not hide extra database calls, retries, or
transport changes behind ordinary methods.

## Client Setup

Legacy code usually created a broad client and let methods choose paths
internally. New code creates one client with explicit safety options:

```go
client, err := openplant.New(
	openplant.WithEndpoint("127.0.0.1", 8200),
	openplant.WithCredentials(user, pass),
)
```

The default client is read-only. Enable writes only for controlled code paths:

```go
client, err := openplant.New(
	openplant.WithEndpoint("127.0.0.1", 8200),
	openplant.WithCredentials(user, pass),
	openplant.WithReadOnly(false),
)
```

## Read API Mapping

Use domain services instead of broad mixed client methods:

| Use case | New API |
| --- | --- |
| metadata points | `client.Metadata().FindPoints(ctx, openplant.MetadataPointQuery{...})` |
| calculation point configs | `client.Metadata().FindCalculationPointConfigs(ctx, openplant.MetadataPointQuery{...})` |
| realtime native read by ID | `client.Realtime().Read(ctx, openplant.RealtimeReadRequest{...})` |
| realtime via SQL/request | `client.Realtime().QuerySQL/QueryRequest(ctx, openplant.RealtimeReadRequest{...})` |
| archive via SQL | `client.Archive().QuerySQL(ctx, openplant.ArchiveQuery{...})` |
| archive via request | `client.Archive().QueryRequest(ctx, openplant.ArchiveQuery{...})` |
| archive via native | `client.Archive().QueryNative(ctx, openplant.ArchiveQuery{...})` |
| archive streaming native | `client.Archive().StreamNative(ctx, query, emit)` |
| stat via SQL/request/native | `client.Stat().QuerySQL/QueryRequest/QueryNative(ctx, query)` |
| active/history alarms | `client.Alarm().ActiveSQL` / `client.Alarm().HistorySQL` |
| OPConsole-style system metrics | `client.System().ReadSQL` / `client.System().HistorySQL` |
| raw readonly SQL | `client.SQL().Query(ctx, query)` |

All time-series queries require an explicit point scope. History, stat, and
historical alarm queries also require a time range.

## Subscriptions

`client.Subscription().Subscribe` is the realtime ID subscription path. It does
not accept GNs and does not resolve them through metadata SQL. Use the
`subscription.GNDriftSource` wrapper with `client.RealtimeSubscriptionIDSource()`
only when the caller explicitly wants metadata-backed GN rebinding.

Point and Alarm table subscriptions are exposed separately as raw-row table
subscriptions:

```go
stream, err := client.Subscription().SubscribeTable(ctx, openplant.TableSubscribeRequest{
	DB:       "W3",
	Table:    "Point",
	Columns:  []string{"ID", "GN", "PN", "RT"},
	Key:      "ID",
	Int32:    []int32{1001},
	Snapshot: true,
})
```

This path is still request/select subscription only. It requires explicit
DB/table/key/index input and returns row maps so applications can decide how to
type table-specific payloads. See `examples/subscription` for typed Point and
Alarm wrappers that stay outside the base SDK.

## Writes

Writes are opt-in and separated by domain:

- `client.Realtime().WriteNative(ctx, openplant.RealtimeWriteRequest{...})`
- `client.Archive().WriteNative(ctx, openplant.ArchiveWriteRequest{...})`
- `client.Archive().DeleteNative(ctx, openplant.ArchiveDeleteRequest{...})`
- `client.Admin().MutateTable(ctx, openplant.TableMutation{...})`

Generic table mutation rejects time-series tables. Use dedicated realtime and
archive write APIs for time-series data.

Native write/delete requests also require IDs. Passing a GN to a native write
surface returns an unsupported-path error instead of doing a hidden metadata
lookup.

## SQL Row Mapping

SQL results remain explicit row maps. Convert only when the caller asks for it:

```go
type PointRow struct {
	ID int32  `openplant:"ID"`
	GN string `openplant:"GN"`
}

rows, err := openplantsql.ScanRows[PointRow](result.Rows)
```

## Status, Alarm, and Security Metadata

The new SDK keeps common OpenPlant bit fields typed instead of requiring every
caller to parse raw integers:

- `openplant.DS` exposes helpers such as `Good`, `Timeout`, `NilV5`,
  `ControlState`, and `AlarmBits`.
- `openplant.AlarmCode` represents LC bitmasks, including analog change alarm
  value `256`.
- `openplant.AlarmPriority` represents the AP priority/color class values
  `0..4`.
- `openplant.SecurityGroups` represents the four-byte Point `SG` bitset and is
  populated by `Metadata().FindPoints`.

These helpers are metadata/status conveniences only. They do not add fallback
or hidden reads.

## Calculation and Mirror Metadata

Calculation formulas remain server-side Lua expressions stored in Point `EX`.
The SDK exposes `calc.Functions` and top-level `openplant.CalcFunctions()` as a
catalog of documented OpenPlant formula functions. It also exposes
`CalcFormulaReferences`, `CalcFormulaUsesFunction`, and `LintCalcFormula` for
authoring tools. These helpers are lexical checks only; they do not execute Lua
or silently rewrite formulas.

OPConsole-style system point templates are available through
`openplant.SystemPointTemplates(db)`. They are reference data for explicit admin
tools and are not written to the server by default.
`BuildSystemPointTemplateInsert`, `BuildSystemPointTemplateReplace`, and
`BuildDefaultSystemPointTemplateInsert` only construct `TableMutation` requests;
the caller must still use a writable client and call `Admin().MutateTable`.
`BuildNodeInsert`, `BuildNodeReplace`, `BuildNodeDelete`,
`BuildPointConfigInsert`, `BuildPointConfigReplace`, and
`BuildPointConfigDelete` follow the same rule for typed Node and Point
configuration rows. Delete builders require discovered positive IDs.
Replicator, RepItem, User, Groups, and Access builders are also available as
explicit `TableMutation` constructors. User builders require credentials from
caller memory; never store real passwords in source, examples, tests, docs, or
committed env files.

For calculation dependency checks, use `BuildCalcDependencyGraph(configs)`.
The graph exposes dependency-first evaluation order, cycles, and `KO`
calculation-order issues without querying the server.

Replication fields now have typed helpers:

- `ReplicationSyncLoose` / `ReplicationSyncPreserveID` for `Replicator.SY`.
- `ReplicationTransformPreserveRole` / `ReplicationTransformCalcAsDAS` for
  `RepItem.XF`.
- `ReplicationBackfillMaxDays` documents the `TL <= 30` mirror backfill limit.

`Replicator.Validate` and `RepItem.Validate` report field-level configuration
issues for callers building admin tools.
`MirrorDiagnose` combines those checks with duplicate source/target detection,
and `MirrorSyncMonitors` discovers calculation points that use
`op.ar_sync_time` or `op.rt_sync_time`.

## Testing During Migration

Run the same safety matrix used by the SDK:

```powershell
go test ./...
go test ./... -p=1 -count=1
go test -tags safe_readonly ./...
go test -tags mutation ./... -p=1 -count=1
```

Mutation tests only run real writes when explicitly enabled with an isolated
namespace. The full lifecycle test creates fresh Node/Point objects, exercises
write/read/update/subscription paths, then deletes the archive samples, Point,
and Node it created.
