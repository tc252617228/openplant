package metadata

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	sqlapi "github.com/tc252617228/openplant/sql"
)

const defaultMetadataLimit = 1000

type Queryer interface {
	Query(ctx context.Context, query string) (sqlapi.Result, error)
}

type Options struct {
	Queryer Queryer
}

type PointQuery struct {
	DB      model.DatabaseName
	IDs     []model.PointID
	GNs     []model.GN
	Prefix  model.GN
	Limit   int
	OrderBy string
}

type NodeQuery struct {
	DB     model.DatabaseName
	IDs    []model.NodeID
	GNs    []model.GN
	Prefix model.GN
	Limit  int
}

type DASQuery struct {
	DB     model.DatabaseName
	IDs    []model.DASID
	GNs    []model.GN
	Prefix model.GN
	Limit  int
}

type DeviceQuery struct {
	DB     model.DatabaseName
	IDs    []model.DeviceID
	GNs    []model.GN
	Prefix model.GN
	Limit  int
}

type ReplicatorQuery struct {
	DB    model.DatabaseName
	Names []string
	Limit int
}

type RepItemQuery struct {
	DB         model.DatabaseName
	PointNames []string
	Limit      int
}

func (q PointQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if len(q.IDs) == 0 && len(q.GNs) == 0 && q.Prefix == "" && q.Limit <= 0 {
		return operror.Validation("metadata.PointQuery.Validate", "unbounded point metadata query requires a positive limit")
	}
	if len(q.IDs) > 0 || len(q.GNs) > 0 {
		if err := (model.PointSelector{IDs: q.IDs, GNs: q.GNs}).ValidateBounded(); err != nil {
			return err
		}
	}
	if q.Prefix != "" {
		if err := q.Prefix.Validate(); err != nil {
			return err
		}
	}
	if q.Limit < 0 {
		return operror.Validation("metadata.PointQuery.Validate", "limit cannot be negative")
	}
	if q.OrderBy != "" {
		if _, err := normalizePointOrderBy(q.OrderBy); err != nil {
			return err
		}
	}
	return nil
}

func (q NodeQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if len(q.IDs) == 0 && len(q.GNs) == 0 && q.Prefix == "" && q.Limit <= 0 {
		return operror.Validation("metadata.NodeQuery.Validate", "unbounded node metadata query requires a positive limit")
	}
	for _, id := range q.IDs {
		if id < 0 {
			return operror.Validation("metadata.NodeQuery.Validate", fmt.Sprintf("node ID cannot be negative: %d", id))
		}
	}
	for _, gn := range q.GNs {
		if err := gn.Validate(); err != nil {
			return err
		}
	}
	if q.Prefix != "" {
		if err := q.Prefix.Validate(); err != nil {
			return err
		}
	}
	if q.Limit < 0 {
		return operror.Validation("metadata.NodeQuery.Validate", "limit cannot be negative")
	}
	return nil
}

func (q DASQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if len(q.IDs) == 0 && len(q.GNs) == 0 && q.Prefix == "" && q.Limit <= 0 {
		return operror.Validation("metadata.DASQuery.Validate", "unbounded DAS metadata query requires a positive limit")
	}
	for _, id := range q.IDs {
		if id < 0 {
			return operror.Validation("metadata.DASQuery.Validate", fmt.Sprintf("DAS ID cannot be negative: %d", id))
		}
	}
	if err := validateGNScope(q.GNs, q.Prefix); err != nil {
		return err
	}
	if q.Limit < 0 {
		return operror.Validation("metadata.DASQuery.Validate", "limit cannot be negative")
	}
	return nil
}

func (q DeviceQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if len(q.IDs) == 0 && len(q.GNs) == 0 && q.Prefix == "" && q.Limit <= 0 {
		return operror.Validation("metadata.DeviceQuery.Validate", "unbounded device metadata query requires a positive limit")
	}
	for _, id := range q.IDs {
		if id < 0 {
			return operror.Validation("metadata.DeviceQuery.Validate", fmt.Sprintf("device ID cannot be negative: %d", id))
		}
	}
	if err := validateGNScope(q.GNs, q.Prefix); err != nil {
		return err
	}
	if q.Limit < 0 {
		return operror.Validation("metadata.DeviceQuery.Validate", "limit cannot be negative")
	}
	return nil
}

func (q ReplicatorQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if len(q.Names) == 0 && q.Limit <= 0 {
		return operror.Validation("metadata.ReplicatorQuery.Validate", "unbounded replicator query requires a positive limit")
	}
	for _, name := range q.Names {
		if strings.TrimSpace(name) == "" {
			return operror.Validation("metadata.ReplicatorQuery.Validate", "replicator name cannot be empty")
		}
	}
	if q.Limit < 0 {
		return operror.Validation("metadata.ReplicatorQuery.Validate", "limit cannot be negative")
	}
	return nil
}

func (q RepItemQuery) Validate() error {
	if err := q.DB.Validate(); err != nil {
		return err
	}
	if len(q.PointNames) == 0 && q.Limit <= 0 {
		return operror.Validation("metadata.RepItemQuery.Validate", "unbounded replication item query requires a positive limit")
	}
	for _, name := range q.PointNames {
		if strings.TrimSpace(name) == "" {
			return operror.Validation("metadata.RepItemQuery.Validate", "replication point name cannot be empty")
		}
	}
	if q.Limit < 0 {
		return operror.Validation("metadata.RepItemQuery.Validate", "limit cannot be negative")
	}
	return nil
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

func (s *Service) FindPoints(ctx context.Context, q PointQuery) ([]model.Point, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("metadata.Service.FindPoints", "SQL queryer is not configured")
	}
	query, err := buildFindPointsSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	points := make([]model.Point, 0, len(result.Rows))
	for _, row := range result.Rows {
		points = append(points, pointFromRow(row))
	}
	return points, nil
}

func (s *Service) FindPointConfigs(ctx context.Context, q PointQuery) ([]model.PointConfig, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("metadata.Service.FindPointConfigs", "SQL queryer is not configured")
	}
	query, err := buildFindPointConfigsSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	points := make([]model.PointConfig, 0, len(result.Rows))
	for _, row := range result.Rows {
		points = append(points, pointConfigFromRow(row))
	}
	return points, nil
}

func (s *Service) FindCalculationPointConfigs(ctx context.Context, q PointQuery) ([]model.PointConfig, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("metadata.Service.FindCalculationPointConfigs", "SQL queryer is not configured")
	}
	query, err := buildFindCalculationPointConfigsSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	points := make([]model.PointConfig, 0, len(result.Rows))
	for _, row := range result.Rows {
		points = append(points, pointConfigFromRow(row))
	}
	return points, nil
}

func (s *Service) ListNodes(ctx context.Context, q NodeQuery) ([]model.Node, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("metadata.Service.ListNodes", "SQL queryer is not configured")
	}
	query, err := buildListNodesSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	nodes := make([]model.Node, 0, len(result.Rows))
	for _, row := range result.Rows {
		nodes = append(nodes, nodeFromRow(row))
	}
	return nodes, nil
}

func (s *Service) ListDAS(ctx context.Context, q DASQuery) ([]model.DAS, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("metadata.Service.ListDAS", "SQL queryer is not configured")
	}
	query, err := buildListDASSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	items := make([]model.DAS, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, dasFromRow(row))
	}
	return items, nil
}

func (s *Service) ListDevices(ctx context.Context, q DeviceQuery) ([]model.Device, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if s.queryer == nil {
		return nil, operror.Unsupported("metadata.Service.ListDevices", "SQL queryer is not configured")
	}
	query, err := buildListDevicesSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.queryer.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	items := make([]model.Device, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, deviceFromRow(row))
	}
	return items, nil
}

func (s *Service) ListProducts(ctx context.Context, limit int) ([]model.Product, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := validateListLimit("metadata.Service.ListProducts", limit); err != nil {
		return nil, err
	}
	result, err := s.queryTable(ctx, "Product", []string{"PJ", "HO", "PN", "ED", "VN", "LI", "SZ", "ET", "AA"}, nil, `"PJ" ASC,"HO" ASC,"PN" ASC`, normalizedListLimit(limit))
	if err != nil {
		return nil, err
	}
	items := make([]model.Product, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, productFromRow(row))
	}
	return items, nil
}

func (s *Service) ListRoots(ctx context.Context, limit int) ([]model.Root, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := validateListLimit("metadata.Service.ListRoots", limit); err != nil {
		return nil, err
	}
	result, err := s.queryTable(ctx, "Root", []string{"ID", "PN", "ED", "IP", "PO", "IO", "WT", "MT", "LG", "SY", "TD", "SD", "CT", "GN", "TM", "AS", "AV"}, []string{`"ID" >= 0`}, `"ID" ASC`, normalizedListLimit(limit))
	if err != nil {
		return nil, err
	}
	items := make([]model.Root, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, rootFromRow(row))
	}
	return items, nil
}

func (s *Service) ListServers(ctx context.Context, limit int) ([]model.Server, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := validateListLimit("metadata.Service.ListServers", limit); err != nil {
		return nil, err
	}
	result, err := s.queryTable(ctx, "Server", []string{"ID", "PN", "ED", "IP", "PO"}, []string{`"ID" >= 0`}, `"ID" ASC`, normalizedListLimit(limit))
	if err != nil {
		return nil, err
	}
	items := make([]model.Server, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, serverFromRow(row))
	}
	return items, nil
}

func (s *Service) ListUsers(ctx context.Context, limit int) ([]model.User, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := validateListLimit("metadata.Service.ListUsers", limit); err != nil {
		return nil, err
	}
	result, err := s.queryTable(ctx, "User", []string{"US"}, nil, `"US" ASC`, normalizedListLimit(limit))
	if err != nil {
		return nil, err
	}
	items := make([]model.User, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, model.User{Name: stringValue(row["US"])})
	}
	return items, nil
}

func (s *Service) ListGroups(ctx context.Context, limit int) ([]model.Group, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := validateListLimit("metadata.Service.ListGroups", limit); err != nil {
		return nil, err
	}
	result, err := s.queryTable(ctx, "Groups", []string{"ID", "GP"}, []string{`"ID" >= 0`}, `"ID" ASC`, normalizedListLimit(limit))
	if err != nil {
		return nil, err
	}
	items := make([]model.Group, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, model.Group{ID: int32Value(row["ID"]), Name: stringValue(row["GP"])})
	}
	return items, nil
}

func (s *Service) ListAccess(ctx context.Context, limit int) ([]model.Access, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := validateListLimit("metadata.Service.ListAccess", limit); err != nil {
		return nil, err
	}
	result, err := s.queryTable(ctx, "Access", []string{"US", "GP", "PL"}, nil, `"US" ASC,"GP" ASC`, normalizedListLimit(limit))
	if err != nil {
		return nil, err
	}
	items := make([]model.Access, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, model.Access{
			User:      stringValue(row["US"]),
			Group:     stringValue(row["GP"]),
			Privilege: stringValue(row["PL"]),
		})
	}
	return items, nil
}

func (s *Service) ListReplicators(ctx context.Context, q ReplicatorQuery) ([]model.Replicator, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	query, err := buildListReplicatorsSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.query(ctx, query)
	if err != nil {
		return nil, err
	}
	items := make([]model.Replicator, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, replicatorFromRow(row))
	}
	return items, nil
}

func (s *Service) ListRepItems(ctx context.Context, q RepItemQuery) ([]model.RepItem, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	query, err := buildListRepItemsSQL(q)
	if err != nil {
		return nil, err
	}
	result, err := s.query(ctx, query)
	if err != nil {
		return nil, err
	}
	items := make([]model.RepItem, 0, len(result.Rows))
	for _, row := range result.Rows {
		items = append(items, repItemFromRow(row))
	}
	return items, nil
}

func (s *Service) ListDatabases(ctx context.Context) ([]model.Database, error) {
	if s.closed != nil {
		return nil, s.closed
	}
	result, err := s.queryTable(ctx, "Database", []string{"ID", "UD", "PN", "ED", "DL", "SL", "PS", "TI", "PD", "FS", "IT", "IL", "AU", "LZ", "MM", "HI", "CT", "GN", "TM", "AS", "AV"}, []string{`"ID" >= 0`}, `"ID" ASC`, defaultMetadataLimit)
	if err != nil {
		return nil, err
	}
	dbs := make([]model.Database, 0, len(result.Rows))
	for _, row := range result.Rows {
		dbs = append(dbs, databaseFromRow(row))
	}
	return dbs, nil
}

func (s *Service) query(ctx context.Context, query string) (sqlapi.Result, error) {
	if s.queryer == nil {
		return sqlapi.Result{}, operror.Unsupported("metadata.Service.query", "SQL queryer is not configured")
	}
	return s.queryer.Query(ctx, query)
}

func (s *Service) queryTable(ctx context.Context, tableName string, columns []string, conditions []string, orderBy string, limit int) (sqlapi.Result, error) {
	query, err := buildListSQL(tableName, columns, conditions, orderBy, limit)
	if err != nil {
		return sqlapi.Result{}, err
	}
	return s.query(ctx, query)
}

func buildListSQL(tableName string, columns []string, conditions []string, orderBy string, limit int) (string, error) {
	table, err := sqlapi.QuoteIdentifier(tableName)
	if err != nil {
		return "", err
	}
	selectCols, err := quoteColumns(columns)
	if err != nil {
		return "", err
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	limitSQL := ""
	if limit > 0 {
		limitSQL = fmt.Sprintf(" LIMIT %d", limit)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s%s",
		strings.Join(selectCols, ","),
		table,
		where,
		orderBy,
		limitSQL,
	), nil
}

func validateListLimit(op string, limit int) error {
	if limit < 0 {
		return operror.Validation(op, "limit cannot be negative")
	}
	return nil
}

func normalizedListLimit(limit int) int {
	if limit <= 0 {
		return defaultMetadataLimit
	}
	return limit
}

func buildListNodesSQL(q NodeQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Node")
	if err != nil {
		return "", err
	}
	columns := []string{"ID", "UD", "ND", "PN", "ED", "FQ", "LC", "AR", "OF", "LO", "CT", "GN"}
	selectCols, err := quoteColumns(columns)
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 3)
	if len(q.IDs) > 0 {
		parts := make([]string, 0, len(q.IDs))
		for _, id := range q.IDs {
			parts = append(parts, fmt.Sprintf("%d", id))
		}
		conditions = append(conditions, fmt.Sprintf(`"ID" IN (%s)`, strings.Join(parts, ",")))
	}
	if len(q.GNs) > 0 {
		parts := make([]string, 0, len(q.GNs))
		for _, gn := range q.GNs {
			parts = append(parts, sqlapi.LiteralString(string(gn)))
		}
		conditions = append(conditions, fmt.Sprintf(`"GN" IN (%s)`, strings.Join(parts, ",")))
	}
	if q.Prefix != "" {
		conditions = append(conditions, fmt.Sprintf(`"GN" LIKE %s`, sqlapi.LiteralLikePrefix(string(q.Prefix))))
	}
	if len(conditions) == 0 {
		conditions = append(conditions, `"ID" >= 0`)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = len(q.IDs) + len(q.GNs)
	}
	if limit <= 0 {
		limit = 1000
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		strings.Join(selectCols, ","),
		table,
		strings.Join(conditions, " AND "),
		`"ID" ASC`,
		limit,
	), nil
}

func buildListDASSQL(q DASQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "DAS")
	if err != nil {
		return "", err
	}
	columns := []string{"ID", "UD", "ND", "PN", "ED", "IP", "PO", "VN", "CT", "GN", "TM", "AS", "AV"}
	selectCols, err := quoteColumns(columns)
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 3)
	if len(q.IDs) > 0 {
		parts := make([]string, 0, len(q.IDs))
		for _, id := range q.IDs {
			parts = append(parts, fmt.Sprintf("%d", id))
		}
		conditions = append(conditions, fmt.Sprintf(`"ID" IN (%s)`, strings.Join(parts, ",")))
	}
	appendGNConditions(&conditions, q.GNs, q.Prefix)
	if len(conditions) == 0 {
		conditions = append(conditions, `"ID" >= 0`)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = len(q.IDs) + len(q.GNs)
	}
	if limit <= 0 {
		limit = 1000
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		strings.Join(selectCols, ","),
		table,
		strings.Join(conditions, " AND "),
		`"ID" ASC`,
		limit,
	), nil
}

func buildListDevicesSQL(q DeviceQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Device")
	if err != nil {
		return "", err
	}
	columns := []string{"ID", "UD", "ND", "CD", "PN", "ED", "CP", "IP", "BA", "LN", "CT", "GN", "TM", "AS", "AV"}
	selectCols, err := quoteColumns(columns)
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 3)
	if len(q.IDs) > 0 {
		parts := make([]string, 0, len(q.IDs))
		for _, id := range q.IDs {
			parts = append(parts, fmt.Sprintf("%d", id))
		}
		conditions = append(conditions, fmt.Sprintf(`"ID" IN (%s)`, strings.Join(parts, ",")))
	}
	appendGNConditions(&conditions, q.GNs, q.Prefix)
	if len(conditions) == 0 {
		conditions = append(conditions, `"ID" >= 0`)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = len(q.IDs) + len(q.GNs)
	}
	if limit <= 0 {
		limit = 1000
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		strings.Join(selectCols, ","),
		table,
		strings.Join(conditions, " AND "),
		`"ID" ASC`,
		limit,
	), nil
}

func buildListReplicatorsSQL(q ReplicatorQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Replicator")
	if err != nil {
		return "", err
	}
	columns := []string{"RN", "IP", "PO", "SP", "SY", "FL", "AR", "TL"}
	selectCols, err := quoteColumns(columns)
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 1)
	if len(q.Names) > 0 {
		conditions = append(conditions, fmt.Sprintf(`"RN" IN (%s)`, literalStringList(q.Names)))
	}
	limit := q.Limit
	if limit <= 0 {
		limit = len(q.Names)
	}
	if limit <= 0 {
		limit = defaultMetadataLimit
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s LIMIT %d",
		strings.Join(selectCols, ","),
		table,
		where,
		`"RN" ASC`,
		limit,
	), nil
}

func buildListRepItemsSQL(q RepItemQuery) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "RepItem")
	if err != nil {
		return "", err
	}
	columns := []string{"PN", "TN", "XF"}
	selectCols, err := quoteColumns(columns)
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 1)
	if len(q.PointNames) > 0 {
		conditions = append(conditions, fmt.Sprintf(`"PN" IN (%s)`, literalStringList(q.PointNames)))
	}
	limit := q.Limit
	if limit <= 0 {
		limit = len(q.PointNames)
	}
	if limit <= 0 {
		limit = defaultMetadataLimit
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s LIMIT %d",
		strings.Join(selectCols, ","),
		table,
		where,
		`"PN" ASC`,
		limit,
	), nil
}

var pointColumns = []string{"ID", "UD", "ND", "PT", "RT", "PN", "AN", "ED", "KR", "SG", "FQ", "LC", "AP", "AR", "EU", "FM", "CT", "EX", "GN"}

var pointConfigColumns = []string{
	"ID", "UD", "ND", "CD", "PT", "RT", "PN", "AN", "ED", "KR", "SG", "FQ",
	"CP", "HW", "BP", "SR", "AD", "LC", "AP", "AR", "OF", "FL", "ST", "RS",
	"EU", "FM", "IV", "BV", "TV", "LL", "HL", "ZL", "ZH", "L3", "H3", "L4",
	"H4", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "DB", "DT", "KZ",
	"TT", "TP", "OT", "KT", "KO", "FK", "FB", "CT", "EX", "GN",
}

func buildFindPointsSQL(q PointQuery) (string, error) {
	return buildFindPointSQL(q, pointColumns, nil)
}

func buildFindPointConfigsSQL(q PointQuery) (string, error) {
	return buildFindPointSQL(q, pointConfigColumns, nil)
}

func buildFindCalculationPointConfigsSQL(q PointQuery) (string, error) {
	return buildFindPointSQL(q, pointConfigColumns, []string{`"PT" = 1`, `"EX" <> ''`})
}

func buildFindPointSQL(q PointQuery, columns []string, extraConditions []string) (string, error) {
	table, err := sqlapi.QualifiedTable(string(q.DB), "Point")
	if err != nil {
		return "", err
	}
	selectCols, err := quoteColumns(columns)
	if err != nil {
		return "", err
	}
	conditions := make([]string, 0, 3)
	if len(q.IDs) > 0 {
		parts := make([]string, 0, len(q.IDs))
		for _, id := range q.IDs {
			parts = append(parts, fmt.Sprintf("%d", id))
		}
		conditions = append(conditions, fmt.Sprintf(`"ID" IN (%s)`, strings.Join(parts, ",")))
	}
	if len(q.GNs) > 0 {
		parts := make([]string, 0, len(q.GNs))
		for _, gn := range q.GNs {
			parts = append(parts, sqlapi.LiteralString(string(gn)))
		}
		conditions = append(conditions, fmt.Sprintf(`"GN" IN (%s)`, strings.Join(parts, ",")))
	}
	if q.Prefix != "" {
		conditions = append(conditions, fmt.Sprintf(`"GN" LIKE %s`, sqlapi.LiteralLikePrefix(string(q.Prefix))))
	}
	conditions = append(conditions, extraConditions...)
	if len(conditions) == 0 {
		conditions = append(conditions, `"ID" >= 0`)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = len(q.IDs) + len(q.GNs)
	}
	if limit <= 0 {
		limit = 1000
	}
	orderBy, err := normalizePointOrderBy(q.OrderBy)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		strings.Join(selectCols, ","),
		table,
		strings.Join(conditions, " AND "),
		orderBy,
		limit,
	), nil
}

func normalizePointOrderBy(orderBy string) (string, error) {
	if strings.TrimSpace(orderBy) == "" {
		return `"GN" ASC`, nil
	}
	allowed := map[string]struct{}{
		"ID": {}, "UD": {}, "ND": {}, "CD": {}, "PT": {}, "RT": {},
		"PN": {}, "AN": {}, "ED": {}, "KR": {}, "SG": {}, "FQ": {},
		"CP": {}, "HW": {}, "BP": {}, "SR": {}, "AD": {}, "LC": {},
		"AP": {}, "AR": {}, "OF": {}, "FL": {}, "ST": {}, "RS": {},
		"EU": {}, "FM": {}, "IV": {}, "BV": {}, "TV": {}, "LL": {},
		"HL": {}, "ZL": {}, "ZH": {}, "L3": {}, "H3": {}, "L4": {},
		"H4": {}, "C1": {}, "C2": {}, "C3": {}, "C4": {}, "C5": {},
		"C6": {}, "C7": {}, "C8": {}, "DB": {}, "DT": {}, "KZ": {},
		"TT": {}, "TP": {}, "OT": {}, "KT": {}, "KO": {}, "FK": {},
		"FB": {}, "CT": {}, "EX": {}, "GN": {},
	}
	terms := strings.Split(orderBy, ",")
	out := make([]string, 0, len(terms))
	for _, term := range terms {
		fields := strings.Fields(strings.TrimSpace(term))
		if len(fields) == 0 || len(fields) > 2 {
			return "", operror.Validation("metadata.PointQuery.OrderBy", "order by must use whitelisted columns with optional ASC/DESC")
		}
		column := strings.Trim(fields[0], `"`)
		if _, ok := allowed[column]; !ok {
			return "", operror.Validation("metadata.PointQuery.OrderBy", "unsupported order by column: "+column)
		}
		direction := "ASC"
		if len(fields) == 2 {
			direction = strings.ToUpper(fields[1])
			if direction != "ASC" && direction != "DESC" {
				return "", operror.Validation("metadata.PointQuery.OrderBy", "order by direction must be ASC or DESC")
			}
		}
		quoted, err := sqlapi.QuoteIdentifier(column)
		if err != nil {
			return "", err
		}
		out = append(out, quoted+" "+direction)
	}
	return strings.Join(out, ","), nil
}

func appendGNConditions(conditions *[]string, gns []model.GN, prefix model.GN) {
	if len(gns) > 0 {
		parts := make([]string, 0, len(gns))
		for _, gn := range gns {
			parts = append(parts, sqlapi.LiteralString(string(gn)))
		}
		*conditions = append(*conditions, fmt.Sprintf(`"GN" IN (%s)`, strings.Join(parts, ",")))
	}
	if prefix != "" {
		*conditions = append(*conditions, fmt.Sprintf(`"GN" LIKE %s`, sqlapi.LiteralLikePrefix(string(prefix))))
	}
}

func validateGNScope(gns []model.GN, prefix model.GN) error {
	for _, gn := range gns {
		if err := gn.Validate(); err != nil {
			return err
		}
	}
	if prefix != "" {
		if err := prefix.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func literalStringList(values []string) string {
	items := make([]string, 0, len(values))
	for _, value := range values {
		items = append(items, sqlapi.LiteralString(value))
	}
	return strings.Join(items, ",")
}

func databaseFromRow(row sqlapi.Row) model.Database {
	return model.Database{
		ID:          int32Value(row["ID"]),
		UUID:        model.UUID(int64Value(row["UD"])),
		Name:        stringValue(row["PN"]),
		Description: stringValue(row["ED"]),
		DataLimit:   int32Value(row["DL"]),
		SaveLimit:   int32Value(row["SL"]),
		PointSize:   int32Value(row["PS"]),
		TimeBase:    int32Value(row["TI"]),
		Period:      int32Value(row["PD"]),
		FileSize:    int32Value(row["FS"]),
		IndexTable:  int32Value(row["IT"]),
		IndexLimit:  int32Value(row["IL"]),
		Auto:        int64Value(row["AU"]) != 0,
		Lazy:        int64Value(row["LZ"]) != 0,
		MemoryMode:  int64Value(row["MM"]) != 0,
		HistoryPath: stringValue(row["HI"]),
		ConfigTime:  timeValue(row["CT"]),
		GN:          model.GN(stringValue(row["GN"])),
		UpdateTime:  timeValue(row["TM"]),
		Status:      int16Value(row["AS"]),
		Value:       int32Value(row["AV"]),
	}
}

func productFromRow(row sqlapi.Row) model.Product {
	return model.Product{
		Project:     stringValue(row["PJ"]),
		Host:        stringValue(row["HO"]),
		Name:        stringValue(row["PN"]),
		Description: stringValue(row["ED"]),
		Version:     stringValue(row["VN"]),
		License:     stringValue(row["LI"]),
		Size:        int32Value(row["SZ"]),
		ExpireTime:  timeValue(row["ET"]),
		Authority:   stringValue(row["AA"]),
	}
}

func rootFromRow(row sqlapi.Row) model.Root {
	return model.Root{
		ID:           int32Value(row["ID"]),
		Name:         stringValue(row["PN"]),
		Description:  stringValue(row["ED"]),
		IP:           stringValue(row["IP"]),
		Port:         int32Value(row["PO"]),
		IOTimeout:    int32Value(row["IO"]),
		WriteTimeout: int32Value(row["WT"]),
		MaxThreads:   int32Value(row["MT"]),
		LogLevel:     int32Value(row["LG"]),
		SyncMode:     int32Value(row["SY"]),
		TimeDiff:     int32Value(row["TD"]),
		StorageDir:   stringValue(row["SD"]),
		ConfigTime:   timeValue(row["CT"]),
		GN:           model.GN(stringValue(row["GN"])),
		UpdateTime:   timeValue(row["TM"]),
		Status:       int16Value(row["AS"]),
		Value:        int32Value(row["AV"]),
	}
}

func serverFromRow(row sqlapi.Row) model.Server {
	return model.Server{
		ID:          int32Value(row["ID"]),
		Name:        stringValue(row["PN"]),
		Description: stringValue(row["ED"]),
		IP:          stringValue(row["IP"]),
		Port:        int32Value(row["PO"]),
	}
}

func replicatorFromRow(row sqlapi.Row) model.Replicator {
	return model.Replicator{
		Name:            stringValue(row["RN"]),
		IP:              stringValue(row["IP"]),
		Port:            int32Value(row["PO"]),
		SourcePort:      int32Value(row["SP"]),
		SyncMode:        model.ReplicationSyncMode(int32Value(row["SY"])),
		FilterUnchanged: int64Value(row["FL"]) != 0,
		ArchiveBackfill: int64Value(row["AR"]) != 0,
		TimeLimitDays:   int32Value(row["TL"]),
	}
}

func repItemFromRow(row sqlapi.Row) model.RepItem {
	return model.RepItem{
		PointName:  stringValue(row["PN"]),
		TargetName: stringValue(row["TN"]),
		Transform:  model.ReplicationTransform(int32Value(row["XF"])),
	}
}

func dasFromRow(row sqlapi.Row) model.DAS {
	return model.DAS{
		ID:          model.DASID(int32Value(row["ID"])),
		UUID:        model.UUID(int64Value(row["UD"])),
		NodeID:      model.NodeID(int32Value(row["ND"])),
		Name:        stringValue(row["PN"]),
		Description: stringValue(row["ED"]),
		IP:          stringValue(row["IP"]),
		Port:        int32Value(row["PO"]),
		Version:     int32Value(row["VN"]),
		ConfigTime:  timeValue(row["CT"]),
		GN:          model.GN(stringValue(row["GN"])),
		UpdateTime:  timeValue(row["TM"]),
		Status:      int16Value(row["AS"]),
		Value:       int32Value(row["AV"]),
	}
}

func deviceFromRow(row sqlapi.Row) model.Device {
	return model.Device{
		ID:          model.DeviceID(int32Value(row["ID"])),
		UUID:        model.UUID(int64Value(row["UD"])),
		NodeID:      model.NodeID(int32Value(row["ND"])),
		DASID:       model.DASID(int32Value(row["CD"])),
		Name:        stringValue(row["PN"]),
		Description: stringValue(row["ED"]),
		Channel:     int32Value(row["CP"]),
		IP:          stringValue(row["IP"]),
		Address:     stringValue(row["BA"]),
		LineName:    stringValue(row["LN"]),
		ConfigTime:  timeValue(row["CT"]),
		GN:          model.GN(stringValue(row["GN"])),
		UpdateTime:  timeValue(row["TM"]),
		Status:      int16Value(row["AS"]),
		Value:       int32Value(row["AV"]),
	}
}

func nodeFromRow(row sqlapi.Row) model.Node {
	return model.Node{
		ID:          model.NodeID(int32Value(row["ID"])),
		UUID:        model.UUID(int64Value(row["UD"])),
		ParentID:    model.NodeID(int32Value(row["ND"])),
		Name:        stringValue(row["PN"]),
		Description: stringValue(row["ED"]),
		Resolution:  int32Value(row["FQ"]),
		AlarmCode:   model.AlarmCode(uint16(int64Value(row["LC"]))),
		Archived:    int64Value(row["AR"]) != 0,
		Offline:     int64Value(row["OF"]) != 0,
		Internal:    int64Value(row["LO"]) != 0,
		ConfigTime:  timeValue(row["CT"]),
		GN:          model.GN(stringValue(row["GN"])),
	}
}

func pointFromRow(row sqlapi.Row) model.Point {
	return model.Point{
		ID:          model.PointID(int32Value(row["ID"])),
		UUID:        model.UUID(int64Value(row["UD"])),
		NodeID:      model.NodeID(int32Value(row["ND"])),
		Source:      model.PointSource(int8Value(row["PT"])),
		Type:        model.PointType(int8Value(row["RT"])),
		Name:        stringValue(row["PN"]),
		Alias:       stringValue(row["AN"]),
		Description: stringValue(row["ED"]),
		Keyword:     stringValue(row["KR"]),
		Security:    securityGroupsValue(row["SG"]),
		Resolution:  int16Value(row["FQ"]),
		AlarmCode:   model.AlarmCode(uint16(int64Value(row["LC"]))),
		AlarmLevel:  model.AlarmPriority(int8Value(row["AP"])),
		Archived:    int64Value(row["AR"]) != 0,
		Unit:        stringValue(row["EU"]),
		Format:      int16Value(row["FM"]),
		ConfigTime:  timeValue(row["CT"]),
		Expression:  stringValue(row["EX"]),
		GN:          model.GN(stringValue(row["GN"])),
	}
}

func pointConfigFromRow(row sqlapi.Row) model.PointConfig {
	return model.PointConfig{
		ID:               model.PointID(int32Value(row["ID"])),
		UUID:             model.UUID(int64Value(row["UD"])),
		NodeID:           model.NodeID(int32Value(row["ND"])),
		DeviceID:         model.DeviceID(int32Value(row["CD"])),
		Source:           model.PointSource(int8Value(row["PT"])),
		Type:             model.PointType(int8Value(row["RT"])),
		Name:             stringValue(row["PN"]),
		Alias:            stringValue(row["AN"]),
		Description:      stringValue(row["ED"]),
		Keyword:          stringValue(row["KR"]),
		Security:         securityGroupsValue(row["SG"]),
		Resolution:       int16Value(row["FQ"]),
		Processor:        int16Value(row["CP"]),
		HardwareAddress:  int32Value(row["HW"]),
		Channel:          int16Value(row["BP"]),
		SignalType:       stringValue(row["SR"]),
		SignalAddress:    stringValue(row["AD"]),
		AlarmCode:        model.AlarmCode(uint16(int64Value(row["LC"]))),
		AlarmLevel:       model.AlarmPriority(int8Value(row["AP"])),
		Archived:         int64Value(row["AR"]) != 0,
		Offline:          int64Value(row["OF"]) != 0,
		Flags:            int32Value(row["FL"]),
		SetDescription:   stringValue(row["ST"]),
		ResetDescription: stringValue(row["RS"]),
		Unit:             stringValue(row["EU"]),
		Format:           int16Value(row["FM"]),
		InitialValue:     float64Value(row["IV"]),
		RangeLower:       float64Value(row["BV"]),
		RangeUpper:       float64Value(row["TV"]),
		Limits: model.AlarmLimits{
			LL: float64Value(row["LL"]),
			HL: float64Value(row["HL"]),
			ZL: float64Value(row["ZL"]),
			ZH: float64Value(row["ZH"]),
			L3: float64Value(row["L3"]),
			H3: float64Value(row["H3"]),
			L4: float64Value(row["L4"]),
			H4: float64Value(row["H4"]),
		},
		Colors: model.AlarmColors{
			LL: int32Value(row["C1"]),
			ZL: int32Value(row["C2"]),
			L3: int32Value(row["C3"]),
			L4: int32Value(row["C4"]),
			HL: int32Value(row["C5"]),
			ZH: int32Value(row["C6"]),
			H3: int32Value(row["C7"]),
			H4: int32Value(row["C8"]),
		},
		Deadband:     float64Value(row["DB"]),
		DeadbandType: model.DeadbandType(int8Value(row["DT"])),
		Compression:  model.PointCompression(int8Value(row["KZ"])),
		StatType:     int8Value(row["TT"]),
		StatPeriod:   int16Value(row["TP"]),
		StatOffset:   int16Value(row["OT"]),
		CalcType:     int8Value(row["KT"]),
		CalcOrder:    int8Value(row["KO"]),
		ScaleFactor:  float64Value(row["FK"]),
		Offset:       float64Value(row["FB"]),
		ConfigTime:   timeValue(row["CT"]),
		Expression:   stringValue(row["EX"]),
		GN:           model.GN(stringValue(row["GN"])),
	}
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

func int8Value(v any) int8   { return int8(int64Value(v)) }
func int16Value(v any) int16 { return int16(int64Value(v)) }
func int32Value(v any) int32 { return int32(int64Value(v)) }

func securityGroupsValue(v any) model.SecurityGroups {
	switch x := v.(type) {
	case model.SecurityGroups:
		return x
	case [4]byte:
		return model.SecurityGroups(x)
	case []byte:
		return model.SecurityGroupsFromBytes(x)
	case string:
		return model.SecurityGroupsFromBytes([]byte(x))
	default:
		return model.SecurityGroups{}
	}
}

func int64Value(v any) int64 {
	switch x := v.(type) {
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case uint:
		return int64(x)
	case float32:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

func float64Value(v any) float64 {
	switch x := v.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case uint8:
		return float64(x)
	case uint16:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case uint:
		return float64(x)
	default:
		return 0
	}
}

func stringValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

func timeValue(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case int32:
		return time.Unix(int64(x), 0)
	case int64:
		return time.Unix(x, 0)
	case float64:
		sec := int64(x)
		nsec := int64(x*1e3) % 1000 * 1e6
		return time.Unix(sec, nsec)
	default:
		return time.Time{}
	}
}
