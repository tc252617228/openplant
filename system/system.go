package system

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tc252617228/openplant/internal/rowconv"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	sqlapi "github.com/tc252617228/openplant/sql"
)

type Metric string

const (
	MetricCacheQueue       Metric = "CACHEQ"
	MetricCalcTime         Metric = "CALCTIME"
	MetricCounter          Metric = "COUNTER"
	MetricDatabaseLoad     Metric = "DBLOAD"
	MetricDatabaseMemory   Metric = "DBMEM"
	MetricDatabaseMemoryPC Metric = "DBMEMPRE"
	MetricEventQueue       Metric = "EVENT"
	MetricIdleThreads      Metric = "IDLE"
	MetricLoad             Metric = "LOAD"
	MetricMemoryFree       Metric = "MEMFREE"
	MetricMemoryFreePC     Metric = "MEMFREEPRE"
	MetricMemoryTotal      Metric = "MEMTOTAL"
	MetricPing             Metric = "PING"
	MetricRate             Metric = "RATE"
	MetricSession          Metric = "SESSION"
	MetricSessionPeak      Metric = "SESSIONPEAK"
	MetricThreads          Metric = "THREAD"
	MetricUptime           Metric = "UPTIME"
	MetricUsedDisk         Metric = "USEDDISK"
	MetricUsedDiskPC       Metric = "USEDDISKPRE"
	MetricVolumeFree       Metric = "VOLFREE"
	MetricVolumeFreePC     Metric = "VOLFREEPRE"
	MetricVolumeTotal      Metric = "VOLTOTAL"
)

type MetricInfo struct {
	Metric      Metric
	Name        string
	Description string
	Unit        string
	Formula     string
}

type Query struct {
	DB      model.DatabaseName
	Metrics []Metric
}

type HistoryQuery struct {
	DB       model.DatabaseName
	Metrics  []Metric
	Range    model.TimeRange
	Interval model.Interval
	Limit    int
}

type MetricSample struct {
	Metric Metric
	Sample model.Sample
}

type Queryer interface {
	Query(ctx context.Context, query string) (sqlapi.Result, error)
}

type Options struct {
	Queryer Queryer
}

type Service struct {
	closed  error
	queryer Queryer
}

func NewService(opts ...Options) *Service {
	s := &Service{}
	if len(opts) > 0 {
		s.queryer = opts[0].Queryer
	}
	return s
}

func NewClosedService(err error) *Service {
	return &Service{closed: err}
}

func DefaultTrendMetrics() []Metric {
	return []Metric{
		MetricSession,
		MetricSessionPeak,
		MetricRate,
		MetricCacheQueue,
		MetricLoad,
		MetricCalcTime,
	}
}

func Metrics() []Metric {
	out := make([]Metric, 0, len(metricCatalog))
	for _, spec := range metricCatalog {
		out = append(out, spec.metric)
	}
	return out
}

func Catalog(db model.DatabaseName) ([]MetricInfo, error) {
	if err := db.Validate(); err != nil {
		return nil, err
	}
	out := make([]MetricInfo, 0, len(metricCatalog))
	for _, spec := range metricCatalog {
		out = append(out, spec.info(db))
	}
	return out, nil
}

func LookupMetric(metric Metric, db model.DatabaseName) (MetricInfo, bool) {
	if err := db.Validate(); err != nil {
		return MetricInfo{}, false
	}
	spec, ok := lookupMetricSpec(metric)
	if !ok {
		return MetricInfo{}, false
	}
	return spec.info(db), true
}

func (m Metric) GN(db model.DatabaseName) model.GN {
	return model.GN(string(db) + ".SYS." + string(m))
}

func (m Metric) Validate() error {
	if _, ok := lookupMetricSpec(m); !ok {
		return operror.Validation("system.Metric.Validate", "unsupported system metric: "+string(m))
	}
	return nil
}

func MetricFromGN(gn model.GN) (Metric, bool) {
	parts := strings.Split(string(gn), ".SYS.")
	if len(parts) != 2 || parts[0] == "" {
		return "", false
	}
	metric := Metric(parts[1])
	if _, ok := lookupMetricSpec(metric); !ok {
		return "", false
	}
	return metric, true
}

func (q Query) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	return validateMetrics(q.Metrics, "system.Query.Validate")
}

func (q HistoryQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if err := validateMetrics(q.Metrics, "system.HistoryQuery.Validate"); err != nil {
		return err
	}
	if err := q.Range.Validate(); err != nil {
		return err
	}
	if err := q.Interval.ValidateRequired(); err != nil {
		return err
	}
	if q.Limit < 0 {
		return operror.Validation("system.HistoryQuery.Validate", "limit cannot be negative")
	}
	return nil
}

func (s *Service) ReadSQL(ctx context.Context, q Query) ([]MetricSample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("system.Service.ReadSQL", "SQL queryer is not configured")
	}
	query, err := buildReadSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return metricSamplesFromRows(result.Rows), nil
}

func (s *Service) HistorySQL(ctx context.Context, q HistoryQuery) ([]MetricSample, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("system.Service.HistorySQL", "SQL queryer is not configured")
	}
	query, err := buildHistorySQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	return metricSamplesFromRows(result.Rows), nil
}

func buildReadSQL(q Query) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Realtime")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns([]string{"ID", "GN", "TM", "DS", "AV", "RT", "FM"})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s",
		strings.Join(columns, ","),
		table,
		metricScopeSQL(q.DB, q.Metrics),
		`"GN" ASC`,
	), nil
}

func buildHistorySQL(q HistoryQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Archive")
	if err != nil {
		return "", err
	}
	columns, err := quoteColumns([]string{"ID", "GN", "TM", "DS", "AV", "RT", "FM"})
	if err != nil {
		return "", err
	}
	conditions := []string{
		metricScopeSQL(q.DB, q.Metrics),
		fmt.Sprintf(`"TM" BETWEEN %s AND %s`, timeLiteral(q.Range.Begin), timeLiteral(q.Range.End)),
		fmt.Sprintf(`"MODE" = %s`, sqlapi.LiteralString(string(model.ModeSpan))),
		fmt.Sprintf(`"INTERVAL" = %s`, sqlapi.LiteralString(string(q.Interval))),
	}
	limit := ""
	if q.Limit > 0 {
		limit = fmt.Sprintf(" LIMIT %d", q.Limit)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s%s",
		strings.Join(columns, ","),
		table,
		strings.Join(conditions, " AND "),
		`"TM" ASC,"GN" ASC`,
		limit,
	), nil
}

func metricSamplesFromRows(rows []sqlapi.Row) []MetricSample {
	out := make([]MetricSample, 0, len(rows))
	for _, row := range rows {
		sample := sampleFromRow(row)
		metric, _ := MetricFromGN(sample.GN)
		out = append(out, MetricSample{Metric: metric, Sample: sample})
	}
	return out
}

func sampleFromRow(row sqlapi.Row) model.Sample {
	value, typ := rowconv.Value(row["AV"])
	if rt := rowconv.Int64(row["RT"]); rt != 0 || row["RT"] != nil {
		if typedValue, ok := rowconv.ValueForType(model.PointType(rt), row["AV"]); ok {
			value = typedValue
			typ = model.PointType(rt)
		}
	}
	return model.Sample{
		ID:     model.PointID(rowconv.Int32(row["ID"])),
		GN:     model.GN(rowconv.String(row["GN"])),
		Type:   typ,
		Format: rowconv.Int16(row["FM"]),
		Time:   rowconv.Time(row["TM"]),
		Status: model.DSFromInt16(rowconv.Int16(row["DS"])),
		Value:  value,
	}
}

func validateMetrics(metrics []Metric, op string) error {
	if len(metrics) == 0 {
		return operror.Validation(op, "at least one system metric is required")
	}
	for _, metric := range metrics {
		if err := metric.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func metricScopeSQL(db model.DatabaseName, metrics []Metric) string {
	items := make([]string, 0, len(metrics))
	for _, metric := range metrics {
		items = append(items, sqlapi.LiteralString(string(metric.GN(db))))
	}
	return fmt.Sprintf(`"GN" IN (%s)`, strings.Join(items, ","))
}

func quoteColumns(columns []string) ([]string, error) {
	out := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted, err := sqlapi.QuoteIdentifier(column)
		if err != nil {
			return nil, err
		}
		out = append(out, quoted)
	}
	return out, nil
}

func timeLiteral(tm time.Time) string {
	tm = tm.Truncate(time.Millisecond)
	layout := "2006-01-02 15:04:05"
	if tm.Nanosecond() != 0 {
		layout = "2006-01-02 15:04:05.000"
	}
	return sqlapi.LiteralString(tm.Format(layout))
}

type metricSpec struct {
	metric      Metric
	name        string
	description string
	unit        string
	formula     func(model.DatabaseName) string
}

func (s metricSpec) info(db model.DatabaseName) MetricInfo {
	return MetricInfo{
		Metric:      s.metric,
		Name:        s.name,
		Description: s.description,
		Unit:        s.unit,
		Formula:     s.formula(db),
	}
}

func lookupMetricSpec(metric Metric) (metricSpec, bool) {
	for _, spec := range metricCatalog {
		if spec.metric == metric {
			return spec, true
		}
	}
	return metricSpec{}, false
}

func formula(text string) func(model.DatabaseName) string {
	return func(model.DatabaseName) string { return text }
}

func dbFormula(format string) func(model.DatabaseName) string {
	return func(db model.DatabaseName) string {
		return strings.ReplaceAll(format, "{{db}}", string(db))
	}
}

var metricCatalog = []metricSpec{
	{MetricCacheQueue, "archive_cache_queue", "Archive cache queue length.", "", dbFormula(`return op.cacheq("{{db}}")`)},
	{MetricCalcTime, "calc_time", "Periodic calculation duration in milliseconds.", "ms", formula("return op.calc_time()")},
	{MetricCounter, "counter", "Database counter value.", "", formula("return op.counter()")},
	{MetricDatabaseLoad, "database_load", "Instant database load.", "%", formula("return op.dbload()")},
	{MetricDatabaseMemory, "database_memory", "Database memory usage.", "MB", formula("return op.dbmem()")},
	{MetricDatabaseMemoryPC, "database_memory_percent", "Database memory usage percent.", "%", dbFormula(`return op.value("{{db}}.SYS.DBMEM") / op.value("{{db}}.SYS.MEMTOTAL") * 100`)},
	{MetricEventQueue, "event_queue", "Realtime event queue length.", "", formula("return op.event()")},
	{MetricIdleThreads, "idle_threads", "Idle thread count.", "", formula("return op.idle()")},
	{MetricLoad, "system_load", "System load percent.", "%", formula("return op.load()")},
	{MetricMemoryFree, "memory_free", "Free system memory.", "MB", formula("return op.memfree()")},
	{MetricMemoryFreePC, "memory_free_percent", "Free system memory percent.", "%", dbFormula(`return op.value("{{db}}.SYS.MEMFREE") / op.value("{{db}}.SYS.MEMTOTAL") * 100`)},
	{MetricMemoryTotal, "memory_total", "Total system memory.", "MB", formula("return op.memtotal()")},
	{MetricPing, "ping", "Ping status for a configured address.", "", formula(`return op.ping("127.0.0.1")`)},
	{MetricRate, "event_rate", "Average event change rate over five seconds.", "", dbFormula(`return op.rate("{{db}}.SYS.EVENT", 5)`)},
	{MetricSession, "session", "Active session count.", "", formula("return op.session()")},
	{MetricSessionPeak, "session_peak", "Peak session count.", "", formula("return op.session_peak()")},
	{MetricThreads, "threads", "Thread count.", "", formula("return op.thread()")},
	{MetricUptime, "uptime", "Database uptime in days.", "day", formula("return op.uptime()")},
	{MetricUsedDisk, "used_disk", "Used database disk space.", "MB", dbFormula(`return op.value("{{db}}.SYS.VOLTOTAL") - op.value("{{db}}.SYS.VOLFREE")`)},
	{MetricUsedDiskPC, "used_disk_percent", "Used database disk space percent.", "%", dbFormula(`return (op.value("{{db}}.SYS.VOLTOTAL") - op.value("{{db}}.SYS.VOLFREE")) / op.value("{{db}}.SYS.VOLTOTAL") * 100`)},
	{MetricVolumeFree, "volume_free", "Free database disk space.", "MB", formula("return op.volfree()")},
	{MetricVolumeFreePC, "volume_free_percent", "Free database disk space percent.", "%", dbFormula(`return op.value("{{db}}.SYS.VOLFREE") / op.value("{{db}}.SYS.VOLTOTAL") * 100`)},
	{MetricVolumeTotal, "volume_total", "Total database disk space.", "MB", formula("return op.voltotal()")},
}
