# OpenPlant SDK Reference

## API Contract

The SDK keeps protocol paths explicit and predictable. Do not put fallback in
default methods:

- `Archive().QuerySQL` and `Stat().QuerySQL`: SQL only.
- `Archive().QueryRequest` and `Stat().QueryRequest`: request/select only.
- `Archive().QueryNative`, `Archive().StreamNative`, `Stat().QueryNative`,
  `Stat().StreamNative`: native only.
- Native base APIs require point IDs. They must not resolve GNs through hidden
  metadata SQL.
- Unsupported native, request, subscription, metadata, or admin behavior returns
  the real error instead of trying another hidden path.

If an application wants layered policy, retries, cross-path reads, or trace, it
should own that behavior outside the base API.

## Safety

- Keep `ReadOnly` default true.
- Write APIs must remain explicit: realtime native write, archive native
  write/delete, and admin table mutation.
- Generic table mutation must not mutate time-series tables.
- Real database tests must use environment variables for credentials.
- Do not commit real hostnames, usernames, passwords, or production point names.
- Mutation tests require opt-in environment variables and isolated object names.

## Public API Checklist

When adding or changing a public API:

- Use typed request structs and typed domain models.
- Require bounded point/time scopes for time-series reads.
- Classify errors through `operror` when crossing SDK boundaries.
- Accept `context.Context` and preserve cancellation.
- Avoid background work that hides retries or extra reads from the caller.
- Add documentation and examples when caller-facing behavior changes.
- Preserve typed V5 semantics for RT/PT/DS/LC/AP/SG. `RT=0` is AX, not
  unknown; point security groups are a four-byte bitset.
- Keep OPConsole-derived system metrics as explicit SQL helpers under
  `system`; do not make them a fallback or metadata-enrichment path.
- Keep raw table subscription explicit. `SubscribeTable` requires DB, table,
  key, and index values, and returns row maps. Typed Point/Alarm conversion
  belongs in examples or application code.
- Keep calculation/Lua support as catalog, lexical helper, and metadata unless a
  future mutation API is explicitly designed and guarded. Never execute Lua in
  the SDK.
- Typed mutation builders may construct `admin.TableMutation` values, but must
  not execute them or bypass readonly checks.
- Node/Point builders should use documented configuration fields only and avoid
  generated fields such as `ID`, `UD`, `CT`, and `GN`.
- User builders may accept credentials from caller memory, but real passwords
  must never be written into source, tests, examples, docs, or committed env
  files.
- Mirror helpers should diagnose caller-provided metadata/config rows and
  formula references. They must not poll, subscribe, or silently query extra
  tables.

## Documentation Checklist

Keep these files current when public behavior changes:

- `README.md`: high-level scope, safety defaults, package map, links.
- `examples/`: compilable usage examples.
- `MIGRATION.md`: legacy-to-new behavior and API mapping.
- `docs-contract.md`: extracted V5 contract notes.
- `benchmarks/README.md`: benchmark entry points.
- `skill/openplant-sdk/`: AI-facing usage and maintenance instructions.

## Validation Matrix

Use these commands before completing broad work:

```powershell
go test ./... -count=1
go test ./... -p=1 -count=1
go test -tags safe_readonly ./tests -count=1
go test -tags safe_readonly ./... -p=1 -count=1
go test -tags mutation ./... -p=1 -count=1
```

For performance work:

```powershell
go test -run '^$' -bench 'Benchmark(StreamNative|DecodeNative|EncodeNative|EncodeRealtime|EncodeArchive|TSValue|DecodeDataSet|RowDecoder|PointCache|ScanRows|PoolAcquireRelease|TableSelect|Subscription|ArchiveQuerySQLRows|StatQuerySQLRows)' -benchtime=100x ./...
```

For soak work:

```powershell
$env:OPENPLANT_TEST_SOAK="1"
$env:OPENPLANT_TEST_SOAK_DURATION_MS="3000"
go test -tags soak ./subscription ./internal/transport -count=1
go test -tags 'safe_readonly soak' ./tests -run TestSafeReadonlySoak -v -count=1
```

Use `docs-contract.md` as the local structured knowledge base for table fields,
DS/LC/AP/SG semantics, SQL mode behavior, calculation function facts,
replication/configuration fields, OPConsole-derived system metrics, and SDK
safety rules.
