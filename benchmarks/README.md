# Benchmarks

Benchmarks live beside the code they measure:

- root package: native Archive/Stat decode, native request encoding, write
  request encoding, and subscription response decode
- `internal/codec`: dataset row decode and TSValue round trips
- `internal/protocol`: table select request encoding
- `archive` and `stat`: SQL row conversion through the public services
- `internal/cache`: point metadata cache lookup and bounded store
- `internal/transport`: connection pool acquire/release on an idle pooled
  connection
- `sql`: typed row scanner

Run all benchmarks:

```powershell
go test -bench . ./...
```

Run the Phase 6 benchmark subset:

```powershell
go test -run '^$' -bench 'Benchmark(StreamNative|DecodeNative|EncodeNative|EncodeRealtime|EncodeArchive|TSValue|DecodeDataSet|RowDecoder|PointCache|ScanRows|PoolAcquireRelease|TableSelect|Subscription|ArchiveQuerySQLRows|StatQuerySQLRows)' -benchtime=100x ./...
```

Use `-benchmem` when comparing optimizations:

```powershell
go test -run '^$' -bench 'Benchmark(PoolAcquireRelease|ScanRows|StreamNative)' -benchmem ./...
```

Run opt-in soak tests:

```powershell
$env:OPENPLANT_TEST_SOAK="1"
$env:OPENPLANT_TEST_SOAK_DURATION_MS="3000"
go test -tags soak ./subscription ./internal/transport -count=1
```

Run real safe-readonly soak tests after setting the normal
`OPENPLANT_TEST_HOST`, `OPENPLANT_TEST_PORT`, `OPENPLANT_TEST_USER`,
`OPENPLANT_TEST_PASS`, `OPENPLANT_TEST_READONLY=1`, and `OPENPLANT_TEST_DB`
environment variables:

```powershell
$env:OPENPLANT_TEST_SOAK="1"
$env:OPENPLANT_TEST_SOAK_DURATION_MS="3000"
go test -tags 'safe_readonly soak' ./tests -run TestSafeReadonlySoak -v -count=1
```

Current known optimization candidate: raw subscription/table decode allocates
per-row maps by design. Keep `SubscribeTable` raw and explicit unless a future
typed example or application layer has a measured need for conversion.
