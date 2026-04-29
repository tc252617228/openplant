# OpenPlant SDK Examples

These examples are intentionally small and environment-driven. They compile with
`go test ./...`, but they only contact a real OpenPlant server when run with
`go run`.

Set connection variables first:

```powershell
$env:OPENPLANT_TEST_HOST="<host>"
$env:OPENPLANT_TEST_PORT="8200"
$env:OPENPLANT_TEST_USER="<user>"
$env:OPENPLANT_TEST_PASS="..."
$env:OPENPLANT_TEST_DB="W3"
$env:OPENPLANT_TEST_POINT_ID="1001"
$env:OPENPLANT_TEST_POINT_GN="W3.N.P1"
$env:OPENPLANT_TEST_PREFIX="SDK_MUTATION_"
```

Use an ignored local env file or the current shell for real values. Do not place
production connection details in examples.

Examples:

- `readonly`: metadata, realtime, safely quoted SQL scan, OPConsole-style
  system metrics, formula reference helpers, and native archive streaming.
- `subscription`: visible subscription event stream.
- `strategy`: application-owned archive native/request/SQL policy with
  inspectable trace; this is intentionally outside the low-level SDK.
- `diagnostics`: offline calculation dependency graph, mirror diagnostics, and
  explicit system/Node point configuration mutation builders.
- `mutation`: dry-run admin builder output by default; real
  `Admin().MutateTable` execution requires `OPENPLANT_TEST_APPLY_ADMIN=1`,
  `OPENPLANT_TEST_MUTATION=1`, `OPENPLANT_TEST_DB`, and an isolated
  `OPENPLANT_TEST_PREFIX`. Optional point/system template inserts require
  `OPENPLANT_TEST_NODE_ID` or `OPENPLANT_TEST_SYSTEM_NODE_ID`.

Calculation dependency graph helpers, mirror diagnostics, and system point
template mutation builders are library helpers; use them in admin tooling before
deciding whether to call `Admin().MutateTable`.

The examples intentionally use bounded point/time scopes. Keep that pattern
when adapting them to a real business database.

Run one example:

```powershell
go run ./examples/readonly
```

Preview mutation builders without connecting to a server:

```powershell
go run ./examples/mutation
```
