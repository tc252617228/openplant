# OpenPlant V5 Contract Notes

Source documents reviewed from `D:\op-project\docs` and
`D:\op-project\docs\数据库接口文档`:

- `openPlant实时数据库介绍V5.0.pdf`
- `数据表结构V5.0.pdf`
- `数据库配置字段说明V5.0.pdf`
- `数据状态说明.pdf`
- `SQL使用说明.pdf`
- `计算函数说明.pdf`
- `Go语言接口说明.pdf`
- `C接口示例.pdf`
- `Java语言接口说明.pdf`

This file is a working contract for the new SDK. It should be updated whenever
protocol behavior is verified against a real V5 server.

## Object Model

OpenPlant V5 organizes time-series objects as a tree:

- root
- database instance, commonly `W3`
- node
- point

The global point name (`GN`) follows the form:

```text
[database].[node].[point]
```

Examples from the product documentation include `W3.UNIT1.TE0001` and
`W3.NCS.AI0001`.

`GN` is the stable business-facing name and should be preferred by high-level
SDK APIs. `ID` is the protocol and storage index. The documents say ID is
assigned at create time and immutable for that object, but operationally a point
delete/recreate can make an old ID drift away from the same business tag. SDK
caches and subscriptions must treat GN-to-ID mapping as refreshable metadata.

## Point Source PT

`PT` describes where point data comes from:

- `0 = DAS`: acquisition point, updated by external collection or calculation
  programs
- `1 = CALC`: calculation point, updated by the database calculation engine

Calculation points are recognized only when `PT=CALC` and the `EX` formula is
configured.

## Point Type RT

`RT` defines the point value type:

| Code | Name | Meaning |
| ---: | --- | --- |
| 0 | AX | single precision analog value |
| 1 | DX | boolean/switch value |
| 2 | I2 | 16-bit integer |
| 3 | I4 | 32-bit integer |
| 4 | R8 | double precision value |
| 5 | LONG | 64-bit integer |
| 6 | TEXT | text value |
| 7 | BLOB | binary value |

SDK invariant: `RT=0` is a valid AX point. Unknown or unspecified type must be
represented as `TypeUnknown = -1`; it must never be represented by zero.

## Main Tables

### Point

Important fields:

- `ID int32`: point identifier
- `UD int64`: UUID
- `ND int32`: parent node ID
- `CD int32`: parent device ID in the configuration field document
- `PT int8`: point source, `DAS` or `CALC`
- `RT int8`: point type
- `PN char(32)`: point name, unique within a node
- `AN char(32)`: alias
- `ED char(60)`: description
- `KR char(16)`: keyword/classification
- `SG binary(4)`: security group bitset
- `FQ int16`: resolution in seconds, affects timeout state
- `CP int16`: processor/device number
- `HW int32`: module or hardware address
- `BP int16`: channel
- `SR char(10)`, `AD char(30)`: signal type/address in configuration docs
- `LC`: alarm configuration
- `AP int8`: alarm priority/color class
- `AR int8`: archive enabled flag
- `OF int8`: offline flag
- `FL int32`: flags
- `ST`, `RS`: descriptions for set/reset values
- `EU char(12)`: engineering unit
- `FM int16`: display precision
- `IV`, `TV`, `BV`: initial value, upper range, lower range
- `LL`, `HL`, `ZL`, `ZH`, `L3`, `H3`, `L4`, `H4`: alarm limits
- `C1` through `C8`: alarm colors in the configuration document
- `DB float`: deadband
- `DT int8`: deadband type, `PCT` or `ENG`
- `KZ int8`: compression type
- `TT`, `TP`, `TO`: statistic-related reserved/config fields
- `KT int8`: calculation type
- `KO int8`: calculation order, V5.0.10+
- `FK`, `FB`: scale factor and offset in the configuration document
- `CT datetime`: configuration time
- `EX string`: calculation formula
- `GN string`: global name

SDK metadata has two Point read shapes. `Metadata().FindPoints` returns a
lightweight point identity/config subset for common lookups.
`Metadata().FindPointConfigs` is an explicit SQL-only full configuration read
that projects the documented Point configuration fields plus OPConsole's `OT`
statistic offset field name. If a target server/version does not expose a
projected configuration column, the server error is returned transparently.

### Node

Important fields:

- `ID int32`
- `UD int64`
- `ND int32`
- `PN char(24)`
- `ED char(60)`
- `FQ int32`
- `LC int32`
- `AR int8`
- `OF int8`
- `LO int8`: internal node flag, `0=No`, `1=Yes`
- `CT datetime`
- `GN string`

Node `LC=1` enables alarm processing under the node.

### Realtime

Fields:

- `ID int32`
- `GN string`
- `TM datetime`
- `DS int16`
- `AV blob/value`

SDK path contract:

- `Realtime().Read` is the native ID-only realtime read path.
- `Realtime().QuerySQL` is the SQL path and may use ID or GN selectors.
- `Realtime().QueryRequest` is the request/select path and may use ID or GN
  indexes. It must not resolve GN through hidden metadata SQL.
- Native realtime responses are decoded from the native ID protocol only. The
  SDK must not populate `Sample.GN` from a metadata cache behind the caller's
  back; callers that require GN should use explicit metadata lookup or a
  SQL/request path that returns `GN`.

The configuration field document also describes the physical dynamic value type
by point type:

| RT | Dynamic `AV` type |
| --- | --- |
| AX | float, 4 bytes |
| DX | byte, 1 byte |
| I2 | short, 2 bytes |
| I4 | int, 4 bytes |
| R8 | double, 8 bytes |
| LONG | long, 8 bytes |
| TEXT | char, variable length |
| BLOB | binary, variable length |

### Archive

Fields:

- `ID int32`
- `GN string`
- `TM datetime`
- `DS int16`
- `AV blob/value`
- hidden `MODE text`
- hidden `INTERVAL text`
- hidden `QTYPE tinyint`

Archive queries must include both point scope and time range. The SQL guide
explicitly warns that history queries must include time period and point
information.

`Archive().SnapshotSQL` is an explicit SQL-only helper for OPConsole-style
historical snapshots. It always uses `MODE='span'`, requires an interval, and
projects `ID,GN,TM,DS,AV,RT,FM` so callers can decode the value type and use the
configured display format. It remains separate from base `QuerySQL`, which keeps
the documented archive projection.

Supported `MODE` values:

- `raw`: raw value with start/end restoration behavior
- `arch`: stored raw archive values
- `span`: equal-spacing values
- `plot`: plot values
- `flow`: accumulated/flow value
- `max`, `min`
- `avg`: area/time weighted average
- `mean`: arithmetic mean
- `stdev`
- `sum`

`INTERVAL` uses duration text such as `1h`, `1m`, `1s`; SQL examples also list
`y`, `d`, `h`, `m`, `s`, `ms`, `w`, and `q`. The interval does not support
fractional values.

`QTYPE`:

- `0`: no filter
- `1`: remove bad-quality points
- `2`: remove timeout points
- `3`: remove both bad-quality and timeout points

### Stat

Fields:

- `ID int32`
- `GN string`
- `TM datetime`
- `DS int16`
- `FLOW double`
- `AVGV double`
- `MAXV double`
- `MINV double`
- `MAXTIME datetime`
- `MINTIME datetime`
- hidden `INTERVAL text`
- hidden `QTYPE tinyint`

Stat queries must also be bounded by points and time range.
The base SDK SQL path selects only the documented Stat columns above. Some
servers expose extra values such as `MEAN` or `SUM`; callers that need those
version-specific fields should use explicit SQL so the behavior is visible.

### Alarm and AAlarm

Realtime alarm table is `Alarm`; historical alarm table is `AAlarm`.

Fields:

- `ID int32`
- `GN string`
- `RT int8`
- `AL int8`
- `AC int32`
- `TF datetime`
- `TA datetime`
- `TM datetime`
- `DS int16`
- `AV blob/value`

OPConsole runtime logs from `opConsole-window/logs/OPConsol.log` show the
console querying active alarms with static point projection fields:

```sql
select ID,GN,PN,AN,ED,EU,TM,TA,TF,AV,DS,RT,AP,LC,C1,C2,C3,C4,C5,C6,C7,C8
from W3.Alarm
where ...
order by TM desc
limit ...
```

The SDK alarm model therefore preserves both the documented dynamic fields and
the OPConsole display/configuration fields when they are projected:

- `PN`, `AN`, `ED`, `EU`
- `AP`
- `LC`
- `C1` through `C8`

`DS` plus `RT`/`LC` is enough to derive the active alarm code for display. The
console renders analog alarm colors from `C1..C8` and uses red for change
alarms.

## OPConsole Error Code Notes

`opConsole-window/conf/locale_zh.ini` and `locale_en.ini` list the user-facing
server/runtime error code messages used by the official control console. The SDK
keeps these as lookup metadata in `operror` so callers can classify server
errors without parsing localized text.

Notable code groups:

- `-96`, `-97`, `-98`, `-99`, `-1001`: connection or network failures where a
  reconnect is normally required.
- `-100..-117`: request/protocol/object/configuration errors returned by the
  OpenPlant service.
- `-200..-202`: archive/history access errors.
- `100..111`: script/VM execution errors.
- `200..206`: TCP/MTable transport errors.
- `410..412`: user/password authentication errors.

The OPConsole locale files contain a duplicated `ErrorCode-11` entry: first
for "function not supported", later for "resource locked". The latter sits
between `-114` and `-116`, so the SDK records it as `-115` rather than copying
the duplicate key literally.

## OPConsole Runtime SQL and UI Notes

The bundled web UI and the logs are useful references for real operational
query shapes. They are not API design templates, but they confirm several V5
contracts:

- Node lists use `select ID,PN,LC,ED,FQ,UD,ND,CT,GN,AR,OF,LO from W3.Node`.
- Point lists commonly use lightweight fields first, then full point
  configuration fields only when the user switches table columns.
- Active alarm polling is SQL-based and includes point static fields projected
  from `Alarm`.
- Trend/chart queries read `Stat` and `Archive` separately; the default
  console configuration uses `trendHisMode=plot`.
- Historical snapshot uses `Archive` `MODE='SPAN'` with an interval such as
  `2s`, first querying distinct timestamps, then reading
  `ID,GN,TM,DS,AV,RT,FM` values ordered by `TM`.
- OPConsole point and alarm filters render `LIKE ... ESCAPE '\'`; this avoids
  treating `_` or `%` in point names/descriptions as wildcards.
- OPConsole's SQL help explicitly demonstrates static point-field projection
  from dynamic tables, for example querying point fields from `Alarm`.
- OPConsole v3.0.0 notes say OpenPlant 5.0.4 `Realtime`, `Archive`, and
  `Alarm` do not support subqueries. SDK SQL helpers should keep generating
  explicit bounded `IN (...)` predicates rather than subquery-dependent forms.

UI formatting facts mirrored by SDK helpers:

- `DS` display order is nil value, timeout, forced, bad, good.
- `DSBIN` is a grouped 16-bit binary string.
- OPConsole treats `DS` analog alarm bits `2,10,3,11,4,12,5,13,15` as
  `LL,ZL,L3,L4,HL,ZH,H3,H4,Change`.
- DX alarm display uses `LC=0` for no alarm, `1` for transition to 0, `2` for
  transition to 1, and `3` for any change.

### System Metrics

OPConsole ships `views/public/static/opconsole/SYS.csv` with calculation point
templates under `DB.SYS.*`. Runtime logs show the console trending these points
through `Archive MODE='span'` using GNs such as:

- `DB.SYS.SESSION`
- `DB.SYS.SESSIONPEAK`
- `DB.SYS.RATE`
- `DB.SYS.CACHEQ`
- `DB.SYS.LOAD`
- `DB.SYS.CALCTIME`

The SDK exposes these as `System().ReadSQL` and `System().HistorySQL`. Both are
explicit SQL helpers: current values read `Realtime`, history reads `Archive`
with `MODE='span'` and a required interval. They do not use native reads,
request/select, metadata lookup, or fallback.

The system metric catalog mirrors OPConsole templates such as `op.cacheq`,
`op.session`, `op.session_peak`, `op.rate`, `op.dbload`, `op.dbmem`,
`op.memfree`, `op.volfree`, `op.voltotal`, `op.uptime`, and `op.calc_time`.
Formula strings are catalog/reference data only; SDK code does not execute Lua.

`SystemPointTemplates` exposes the same `DB.SYS.*` calculation point templates
as structured `PointTemplate` values. `PointTemplate.PointConfig()` is a local
conversion helper for explicit admin tooling; the SDK does not insert or update
those points automatically.

`BuildSystemPointTemplateInsert` and related helpers build explicit
`admin.TableMutation` requests for those templates. They do not bypass readonly
mode and do not execute the mutation. The caller must still opt in with
`WithReadOnly(false)` and call `Admin().MutateTable`.

`BuildNodeInsert`/`BuildNodeReplace`/`BuildNodeDelete` and
`BuildPointConfigInsert`/`BuildPointConfigReplace`/`BuildPointConfigDelete` are
typed builders for stable Node and Point configuration fields. They construct
`admin.TableMutation` values only; they do not write to the server or resolve
IDs/GNs. Delete builders are ID-bounded and require positive IDs discovered from
metadata after creation.
Builders also exist for Replicator, RepItem, User, Groups, and Access. User
builders accept credentials from caller memory. Real passwords must not be
stored in source, tests, examples, docs, or committed environment files.

## Subscription

The Java interface manual documents `OPSubscribe(conn, tableName, keyList,
callback)` for `Realtime`, `Alarm`, and `Point`. The key list may contain point
IDs or point names. The Go interface manual shows the same subscription shape
through `Subscribe(table, keys, callback)` and explicit add/remove calls.

Verified SDK policy:

- `Client.Subscription()` is the default realtime ID subscription path. It
  encodes a request/select subscription against `<db>.Realtime` with key `ID`,
  `Async=1`, and the realtime columns `ID, GN, TM, DS, AV`.
- The default source rejects GN selectors before opening a subscription
  connection. It does not resolve GNs through hidden metadata SQL.
- Reconnect and resubscribe stay on the same request/select subscription path;
  status is surfaced as subscription events.
- GN drift handling belongs to the explicit `subscription.GNDriftSource`
  wrapper with `client.RealtimeSubscriptionIDSource()` as its ID source, where
  the caller can see that metadata resolution and rebinding are part of the
  selected behavior.
- `Subscription().SubscribeTable` is the explicit raw-row table subscription
  path. It encodes `<db>.<table>` request/select subscriptions with caller
  supplied columns, key, index values, `Async=1`, and optional `Snapshot`.
- Real V5 verification on 2026-04-29 confirmed snapshot payload decoding for
  `Point` with columns `ID, GN, PN, RT, ED`, and for `Alarm` with OPConsole
  projected columns `ID, GN, PN, AN, ED, EU, TM, TA, TF, AV, DS, RT, AP, LC,
  C1..C8`.

Boundaries:

- Alarm and Point typed subscription wrappers are not part of the default
  realtime subscription path or low-level SDK API. The example-level wrappers
  are thin conversions over `SubscribeTable` with documented projections and no
  hidden GN-to-ID lookup.
- Raw table subscription is not a mutation, fallback, or metadata enrichment
  path. If the target table or key is unsupported, return the request/select
  subscription error transparently.

### User, Groups, Access

- `User`: `US text`, `PW text`
- `Groups`: `GP text`, `ID int`; negative IDs are not changeable
- `Access`: `US text`, `GP int/text`, `PL text`

Passwords must never be written into SDK source, examples, tests, or docs.

### Product, Root, Server, Database, DAS, Device, Replicator, RepItem

Configuration documents describe these tables as metadata/configuration
surfaces. The SDK should default to read APIs first and put writes under the
`admin` package with readonly and mutation-test guards.

Key fields:

- `Product`: `PJ`, `HO`, `PN`, `ED`, `VN`, `LI`, `SZ`, `ET`, `AA`
- `Root`: `ID`, `PN`, `ED`, `IP`, `PO`, `IO`, `WT`, `MT`, `LG`, `SY`, `TD`,
  `SD`, `CT`, `GN`, `TM`, `AS`, `AV`
- `Server`: `ID`, `PN`, `ED`, `IP`, `PO`
- `Database`: `ID`, `UD`, `PN`, `ED`, `DL`, `SL`, `PS`, `TI`, `PD`, `FS`,
  `IT`, `IL`, `AU`, `LZ`, `MM`, `HI`, `CT`, `GN`, `TM`, `AS`, `AV`
- `DAS`: `ID`, `UD`, `ND`, `PN`, `ED`, `IP`, `PO`, `VN`, `CT`, `GN`, `TM`,
  `AS`, `AV`
- `Device`: `ID`, `UD`, `ND`, `CD`, `PN`, `ED`, `CP`, `IP`, `BA`, `LN`, `CT`,
  `GN`, `TM`, `AS`, `AV`
- `Replicator`: `RN`, `IP`, `PO`, `SP`, `SY`, `FL`, `AR`, `TL`
- `RepItem`: `PN`, `TN`, `XF`

Replication fields:

- `XF=0`: preserve calculation/acquisition point roles
- `XF=1`: mirror calculation points as acquisition points
- `SY=0`: destination and source IDs are not strictly checked
- `SY=1`: try to preserve source IDs
- `FL=1`: traffic filter for unchanged data, periodic send every 30 seconds
- `AR=1`: history backfill enabled
- `AR=0`: history backfill disabled
- `TL`: backfill limit in days, documented maximum 30 days

SDK typed helpers:

- `ReplicationSyncLoose = 0`
- `ReplicationSyncPreserveID = 1`
- `ReplicationTransformPreserveRole = 0`
- `ReplicationTransformCalcAsDAS = 1`
- `ReplicationBackfillMaxDays = 30`

`Replicator.Validate` and `RepItem.Validate` are local configuration checks for
admin tooling. They do not query or mutate the server.

The `mirror` package provides local diagnostics over already-read
Replicator/RepItem rows and calculation point configs. It can discover formulas
that use `op.ar_sync_time` and `op.rt_sync_time`, but it does not subscribe,
poll, or query mirror state implicitly.

### Configuration Files

`config.xml` fields describe server-level defaults. Important values for SDK
timeouts, integration tests, and admin surfaces:

- `port`: service port, default `8200`
- `timeout`: network timeout in seconds, default `60`
- `waitTimeout`: idle network timeout in seconds, default `1800`
- `maxThreads`: maximum server worker threads; root table docs mention a hard
  maximum of `5000`
- `logLevel`: log level
- `timeSync`: `0` uses source timestamps, `1` uses server time
- `timediff`: maximum allowed difference between source time and server time
  for realtime writes; default `28800` seconds, `0` disables the check
- `secondDir`: secondary archive directory
- `diskLimit` and `secLimit`: online storage percentage limits
- `pageSize`: archive page size, default `4096`; configuration docs recommend
  `16384` for millisecond workloads
- `archivePeriod`: archive period, documented as days in config docs and as
  seconds in the product overview; verify against the target server before
  exposing write controls
- `archiveSize`: archive file size in MB, default `1500`
- `archiveInterval`: cache-to-history flush interval in seconds
- `autoDeadband`: `0` disabled, `1` enabled
- `lossless`: `0` removes duplicate values during lossless compression, `1`
  keeps duplicates
- `indexLimit`, `indexTime`: archive index cache sizing

`replicator.xml` contains connection credentials for mirror targets. The SDK
must never persist these credentials and should only expose safe readonly
inspection unless mutation is explicitly enabled.

## DS Status Word

Dynamic realtime and history values include `TM`, `DS`, and `AV`. `DS` is a
16-bit status word.

Known bits:

- bit 0: DX value
- bits 1-4: alarm level/detail bits
- bit 5: unacknowledged alarm
- bit 6: alarm suppression
- bit 7: in alarm / abnormal state
- bit 8: device forced state
- bit 9: quality, `0=good`, `1=bad`
- bits 10-11: control command state
  - `11`: control timeout
  - `01`: control issued
  - `10`: control fault
- bit 12: has control command
- bit 13: device tag/挂牌
- bit 14: initial value after database startup
- bit 15: timeout

When bits 9, 14, and 15 are all set, V5 represents a nil value.

Timeout is related to data updates and point resolution `FQ`. The documentation
notes that timeout polling is periodic, so timeout may be set after the
resolution threshold plus the server polling delay. The status document says
the polling delay is typically 60-120 seconds.

Alarm status details:

- bit 7 set means abnormal/in alarm for both analog and DX points
- bit 5 set means the alarm is unacknowledged
- for analog points while bit 7 is set, bits 1-4 encode the active alarm
  detail:
  - `0100`: high limit `HL`
  - `1100`: high-high `ZH`
  - `0101`: high 3 `H3`
  - `1101`: high 4 `H4`
  - `0010`: low limit `LL`
  - `1010`: low-low `ZL`
  - `0011`: low 3 `L3`
  - `1011`: low 4 `L4`
  - `1111`: change alarm
- when bit 7 is clear and bit 5 is set, the point has returned to normal but
  the previous alarm is still unacknowledged

## LC Alarm Configuration

Analog types `AX`, `I2`, `I4`, `R8`, and `LONG` support eight-limit alarms and
change alarm. LC is a bitmask:

- `1`: LL
- `2`: HL
- `4`: ZL
- `8`: ZH
- `16`: L3
- `32`: H3
- `64`: L4
- `128`: H4
- `256`: change alarm

DX LC values:

- `0`: no alarm event
- `1`: alarm on transition to 0
- `2`: alarm on transition to 1
- `3`: alarm on any change

The SDK uses a wide unsigned type for LC masks because the documented analog
change alarm value is `256`, which does not fit in a signed int8.

### Alarm Colors and Priority

Point configuration fields `C1` through `C8` hold the default color for each
analog limit. The configuration field document lists:

| Field | Limit | Default |
| --- | --- | --- |
| `C1` | `LL` | `0xFF0000` |
| `C2` | `ZL` | `0xCC0000` |
| `C3` | `L3` | `0x990000` |
| `C4` | `L4` | `0x660000` |
| `C5` | `HL` | `0xFF0000` |
| `C6` | `ZH` | `0xCC0000` |
| `C7` | `H3` | `0x990000` |
| `C8` | `H4` | `0x660000` |

The standalone status document has a conflicting table for the high-side
colors (`HL=0xCC0000`, `ZH=0x990000`, `H3=0xCC0000`). Treat configured table
fields or live server values as authoritative and avoid baking high-side color
assumptions into behavior-sensitive code.

Point `AP` represents alarm priority/color class:

- `0`: unconfigured
- `1`: red
- `2`: yellow
- `3`: white
- `4`: green

### Security Groups

`Groups` stores security group names and IDs. Negative group IDs are system
owned or not changeable. The `Point.SG` field is a four-byte bitset:

- security group IDs `0..7` are represented by bits in the first byte
- group ID `0` sets `SG[0]` bit `0`, value `1`
- group ID `1` sets `SG[0]` bit `1`, value `2`
- group ID `7` sets `SG[0]` bit `7`, value `128`
- SQL helper functions include `bit(SG,n)`, `bitset(SG,n)`, and `bitclr(SG,n)`

Security groups are selection and permission aids. They do not change the point
tree parentage; a point remains under its original node.

## SQL Table Capability Notes

The SQL guide documents version-specific mutation support. The SDK policy is
stricter than server capability: default public reads remain readonly, and
writes require explicit mutation APIs.

V5.0.4 and later:

| Table | SELECT | INSERT | UPDATE | DELETE |
| --- | --- | --- | --- | --- |
| `Database` | yes | yes | yes | yes |
| `Node` | yes | yes | yes | yes |
| `Point` | yes | yes | yes | yes |
| `Realtime` | yes | yes | yes | no |
| `Archive` | yes | yes | yes | yes |
| `Stat` | yes | no | no | no |
| `Alarm` | yes | no | no | no |
| `AAlarm` | yes | no | no | no |
| `User` | yes | yes | yes | yes |
| `Groups` | yes | yes | yes | yes |
| `Access` | yes | yes | yes | yes |

The SQL examples show these operators and expressions as supported:

- `IN`, `NOT IN`, comparisons, `BETWEEN`
- `LIKE`, `NOT LIKE`, `REGEXP`, `ESCAPE`
- bit operators such as `&`, `|`, `~`, `<<`, `>>`
- arithmetic operators `+`, `-`, `*`, `/`
- `AS` aliases, `ORDER BY`, `LIMIT`, `count`
- `strftime` for timestamp comparisons
- static point fields can be projected from dynamic tables, for example alarm
  and realtime queries can include fields such as `PN`, `TV`, or `ED`

## SQL Safety Rules

The SQL guide says:

- `select` should include `where`
- history queries must include point scope and time range
- realtime `update` only updates specified fields
- Archive raw mode returns boundary-restored values when no original sample
  exists exactly at the start/end; `arch` returns stored original archive
  samples only
- `MODE` values include `raw`, `arch`, `span`, `plot`, `flow`, `max`, `min`,
  `avg`, `mean`, `stdev`, and `sum`
- `INTERVAL` must be an integer amount plus unit; examples include `y`, `q`,
  `w`, `d`, `h`, `m`, `s`, and `ms`

SDK policy:

- `SQL().Query` accepts only readonly statements.
- `SQL().ExecUnsafe` is explicit and requires `AllowUnsafeSQL`.
- `ReadOnly` mode blocks all unsafe execution.
- Archive, Stat, and AAlarm SQL helpers must require point scope and time range.
- DB/table identifiers must be validated or quoted by SDK helpers.
- User-provided values must be escaped or represented through structured
  builders.
- User-provided `LIKE` fragments must escape `\`, `%`, and `_` and render an
  explicit `ESCAPE '\'` clause. OPConsole logs and changelogs show this matters
  for point/alarm filters containing underscores.
- Native SDK methods require explicit point IDs. They do not resolve GNs or
  enrich native results through metadata SQL, metadata cache, or request/select
  behind the caller's back.
- Request/select SDK methods may use ID or GN indexes, but they must stay on
  the request/select path and return server unsupported errors transparently.
- OpenPlant V5 accepts database-qualified table names in the form
  `W3.Point`. A real V5 target returned `-116 resource not available` for the
  SQLite-style split-quoted form `"W3"."Point"`, so SDK SQL builders validate
  DB/table identifiers but render qualified OpenPlant table names without
  split quotes.

## Calculation Points

Calculation service facts:

- Only `PT=CALC` points with configured `EX` formulas are calculated.
- `KT` controls change calculation versus per-second calculation.
- `KO` controls calculation order when multiple calculation points depend on
  each other.
- The server exposes the internal variable `op` in calculation formulas; users
  must not redefine it.
- Formula syntax supports arithmetic, logic, comparison, Lua statements, Lua
  standard library, OpenPlant functions, and water/steam functions.
- Text and binary types do not support max/min-style statistic functions.
- Calculation-point output status follows input quality:
  - all referenced points good -> calculation point good
  - any referenced point timeout -> calculation point bad
  - all referenced points timeout -> calculation point timeout

The SDK should expose calculation metadata as point fields first. Formula
authoring and mutation APIs belong under controlled admin/mutation surfaces.
`Metadata().FindCalculationPointConfigs` is the explicit readonly discovery path
for calculated points. It reads `Point` with `PT=CALC` and non-empty `EX`, and
still requires an explicit point scope or limit.

Important OpenPlant calculation functions from the docs:

- point snapshots: `op.value`, `op.status`, `op.time`, `op.get`,
  `op.dynamic`
- status methods: `good`, `bad`, `alarm`, `level`, `inhibit`, `unack`
- history snapshots: `op.snapshot` with `prev`, `inter`, `next`, `near`, and
  `none`; `op.prev`; `op.next`
- history series: `op.archive`, `op.plot`, `op.span`
- statistics: `op.stat`, `op.max`, `op.min`, `op.avg`, `op.mean`, `op.sum`,
  `op.flow`; `op.stdev` is documented as not implemented
- system values: `op.cacheq`, `op.dbload`, `op.dbmem`, `op.volfree`,
  `op.voltotal`, `op.uptime`, `op.calc_time`
- time helpers: `op.now`, `op.today`, `op.date`, `op.bday`, `op.bmonth`,
  `op.bnextmonth`, `op.timeadd`, `op.timediff`, `op.year`, `op.month`,
  `op.day`, `op.hour`, `op.minute`, `op.second`, `op.msecond`, `op.format`
- write from calculation: `op.set`
- periodic calculation helpers: `op.acc`, `op.meter`, `op.meterp`,
  `op.calctime`, `op.calcmaxtime`, `op.calcmaxtag`
- mirror monitoring: `op.ar_sync_time`, `op.rt_sync_time`
- water/steam functions are provided in `if97.*` and `ifc67.*` families for
  pressure, temperature, enthalpy, entropy, volume, heat capacity, viscosity,
  and related thermodynamic calculations

The SDK `calc` package keeps this as a catalog for formula authoring tools:
function name, category, signature, documentation notes, and whether the docs
mark the function implemented. It intentionally does not parse or run formulas.
It additionally provides lexical helpers to extract quoted point references and
lint for obvious authoring mistakes such as an empty formula, unterminated
strings/comments, redefining the reserved `op` object, or calling an unknown
`op.*` function.

`BuildDependencyGraph` consumes caller-provided `PointConfig` values and builds
a local dependency graph from formula references. It reports internal/external
references, evaluation order, dependency cycles, and calculation-order issues
when a referenced calculation point has `KO` greater than or equal to the
dependent point.
