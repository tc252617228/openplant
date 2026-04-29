package openplant

import (
	"github.com/tc252617228/openplant/admin"
	"github.com/tc252617228/openplant/alarm"
	"github.com/tc252617228/openplant/archive"
	"github.com/tc252617228/openplant/calc"
	"github.com/tc252617228/openplant/metadata"
	"github.com/tc252617228/openplant/mirror"
	"github.com/tc252617228/openplant/model"
	"github.com/tc252617228/openplant/operror"
	"github.com/tc252617228/openplant/realtime"
	"github.com/tc252617228/openplant/stat"
	"github.com/tc252617228/openplant/subscription"
	"github.com/tc252617228/openplant/system"
)

type (
	DatabaseName         = model.DatabaseName
	GN                   = model.GN
	PointID              = model.PointID
	NodeID               = model.NodeID
	DASID                = model.DASID
	DeviceID             = model.DeviceID
	UUID                 = model.UUID
	PointSource          = model.PointSource
	PointType            = model.PointType
	Value                = model.Value
	DS                   = model.DS
	DSQualityState       = model.DSQualityState
	AlarmState           = model.AlarmState
	AlarmCode            = model.AlarmCode
	AlarmPriority        = model.AlarmPriority
	AlarmLimits          = model.AlarmLimits
	AlarmColors          = model.AlarmColors
	DeadbandType         = model.DeadbandType
	PointCompression     = model.PointCompression
	SecurityGroups       = model.SecurityGroups
	TimeRange            = model.TimeRange
	Interval             = model.Interval
	ArchiveMode          = model.ArchiveMode
	QualityFilter        = model.QualityFilter
	Sample               = model.Sample
	StatSample           = model.StatSample
	Point                = model.Point
	PointConfig          = model.PointConfig
	Node                 = model.Node
	DAS                  = model.DAS
	Device               = model.Device
	Database             = model.Database
	Product              = model.Product
	Root                 = model.Root
	Server               = model.Server
	User                 = model.User
	Group                = model.Group
	Access               = model.Access
	Replicator           = model.Replicator
	RepItem              = model.RepItem
	ReplicationSyncMode  = model.ReplicationSyncMode
	ReplicationTransform = model.ReplicationTransform
	ReplicationIssue     = model.ReplicationIssue
	ReplicationIssues    = model.ReplicationIssues
	AlarmRecord          = model.Alarm

	ArchiveQuery            = archive.Query
	ArchiveSnapshotQuery    = archive.SnapshotQuery
	ArchiveWriteRequest     = archive.WriteRequest
	ArchiveDeleteRequest    = archive.DeleteRequest
	StatQuery               = stat.Query
	RealtimeReadRequest     = realtime.ReadRequest
	RealtimeWrite           = realtime.Write
	RealtimeWriteRequest    = realtime.WriteRequest
	MetadataPointQuery      = metadata.PointQuery
	MetadataNodeQuery       = metadata.NodeQuery
	MetadataDASQuery        = metadata.DASQuery
	MetadataDeviceQuery     = metadata.DeviceQuery
	MetadataReplicatorQuery = metadata.ReplicatorQuery
	MetadataRepItemQuery    = metadata.RepItemQuery
	AlarmHistoryQuery       = alarm.HistoryQuery
	SubscribeRequest        = subscription.Request
	SubscribeEventKind      = subscription.EventKind
	SubscribeEvent          = subscription.Event
	SubscribeStream         = subscription.Stream
	TableSubscribeRequest   = subscription.TableRequest
	TableSubscribeEvent     = subscription.TableEvent
	TableSubscribeStream    = subscription.TableStream
	SystemMetric            = system.Metric
	SystemMetricInfo        = system.MetricInfo
	SystemQuery             = system.Query
	SystemHistoryQuery      = system.HistoryQuery
	SystemMetricSample      = system.MetricSample
	SystemPointTemplate     = system.PointTemplate
	CalcFunctionCategory    = calc.FunctionCategory
	CalcFunction            = calc.Function
	CalcFormulaIssueKind    = calc.FormulaIssueKind
	CalcFormulaIssue        = calc.FormulaIssue
	CalcDependencyGraph     = calc.DependencyGraph
	CalcDependencyNode      = calc.DependencyNode
	CalcDependencyCycle     = calc.DependencyCycle
	CalcOrderIssue          = calc.OrderIssue
	MirrorSyncKind          = mirror.SyncKind
	MirrorSeverity          = mirror.Severity
	MirrorIssue             = mirror.Issue
	MirrorConfig            = mirror.Config
	MirrorSyncMonitor       = mirror.SyncMonitor

	Error           = operror.Error
	ErrorKind       = operror.Kind
	ServerCode      = operror.ServerCode
	ServerCodeClass = operror.ServerCodeClass
	ServerCodeInfo  = operror.ServerCodeInfo

	MutationAction     = admin.MutationAction
	MutationColumn     = admin.Column
	MutationColumnType = admin.ColumnType
	MutationFilter     = admin.Filter
	MutationIndexes    = admin.Indexes
	MutationRow        = admin.Row
	UserCredential     = admin.UserCredential
	TableMutation      = admin.TableMutation
)

const (
	SourceDAS  = model.SourceDAS
	SourceCalc = model.SourceCalc

	TypeUnknown = model.TypeUnknown
	TypeAX      = model.TypeAX
	TypeDX      = model.TypeDX
	TypeI2      = model.TypeI2
	TypeI4      = model.TypeI4
	TypeR8      = model.TypeR8
	TypeI8      = model.TypeI8
	TypeTX      = model.TypeTX
	TypeBN      = model.TypeBN

	DSDXValue      = model.DSDXValue
	DSAlarmBit1    = model.DSAlarmBit1
	DSAlarmBit2    = model.DSAlarmBit2
	DSAlarmBit3    = model.DSAlarmBit3
	DSAlarmBit4    = model.DSAlarmBit4
	DSUnackedAlarm = model.DSUnackedAlarm
	DSAlarmBlocked = model.DSAlarmBlocked
	DSInAlarm      = model.DSInAlarm
	DSForced       = model.DSForced
	DSBadQuality   = model.DSBadQuality
	DSControlBit0  = model.DSControlBit0
	DSControlBit1  = model.DSControlBit1
	DSHasControl   = model.DSHasControl
	DSDeviceTagged = model.DSDeviceTagged
	DSInitial      = model.DSInitial
	DSTimeout      = model.DSTimeout

	DSQualityGood    = model.DSQualityGood
	DSQualityBad     = model.DSQualityBad
	DSQualityForced  = model.DSQualityForced
	DSQualityTimeout = model.DSQualityTimeout
	DSQualityNull    = model.DSQualityNull

	AlarmStateNormal          = model.AlarmStateNormal
	AlarmStateActive          = model.AlarmStateActive
	AlarmStateRestoredUnacked = model.AlarmStateRestoredUnacked

	AlarmNone   = model.AlarmNone
	AlarmLL     = model.AlarmLL
	AlarmHL     = model.AlarmHL
	AlarmZL     = model.AlarmZL
	AlarmZH     = model.AlarmZH
	AlarmL3     = model.AlarmL3
	AlarmH3     = model.AlarmH3
	AlarmL4     = model.AlarmL4
	AlarmH4     = model.AlarmH4
	AlarmChange = model.AlarmChange

	AlarmPriorityUnset  = model.AlarmPriorityUnset
	AlarmPriorityRed    = model.AlarmPriorityRed
	AlarmPriorityYellow = model.AlarmPriorityYellow
	AlarmPriorityWhite  = model.AlarmPriorityWhite
	AlarmPriorityGreen  = model.AlarmPriorityGreen

	DXAlarmToZero = model.DXAlarmToZero
	DXAlarmToOne  = model.DXAlarmToOne
	DXAlarmToggle = model.DXAlarmToggle

	AlarmLimitMask  = model.AlarmLimitMask
	AlarmAnalogMask = model.AlarmAnalogMask

	DeadbandPCT = model.DeadbandPCT
	DeadbandENG = model.DeadbandENG

	PointCompressionDeadband = model.PointCompressionDeadband
	PointCompressionLinear   = model.PointCompressionLinear
	PointCompressionNone     = model.PointCompressionNone

	ReplicationSyncLoose             = model.ReplicationSyncLoose
	ReplicationSyncPreserveID        = model.ReplicationSyncPreserveID
	ReplicationTransformPreserveRole = model.ReplicationTransformPreserveRole
	ReplicationTransformCalcAsDAS    = model.ReplicationTransformCalcAsDAS
	ReplicationBackfillMaxDays       = model.ReplicationBackfillMaxDays

	ModeRaw   = model.ModeRaw
	ModeArch  = model.ModeArch
	ModeSpan  = model.ModeSpan
	ModePlot  = model.ModePlot
	ModeFlow  = model.ModeFlow
	ModeMax   = model.ModeMax
	ModeMin   = model.ModeMin
	ModeAvg   = model.ModeAvg
	ModeMean  = model.ModeMean
	ModeStDev = model.ModeStDev
	ModeSum   = model.ModeSum

	QualityNone              = model.QualityNone
	QualityDropBad           = model.QualityDropBad
	QualityDropTimeout       = model.QualityDropTimeout
	QualityDropBadAndTimeout = model.QualityDropBadAndTimeout

	SubscribeEventData         = subscription.EventData
	SubscribeEventError        = subscription.EventError
	SubscribeEventReconnecting = subscription.EventReconnecting
	SubscribeEventReconnected  = subscription.EventReconnected

	SystemMetricCacheQueue       = system.MetricCacheQueue
	SystemMetricCalcTime         = system.MetricCalcTime
	SystemMetricCounter          = system.MetricCounter
	SystemMetricDatabaseLoad     = system.MetricDatabaseLoad
	SystemMetricDatabaseMemory   = system.MetricDatabaseMemory
	SystemMetricDatabaseMemoryPC = system.MetricDatabaseMemoryPC
	SystemMetricEventQueue       = system.MetricEventQueue
	SystemMetricIdleThreads      = system.MetricIdleThreads
	SystemMetricLoad             = system.MetricLoad
	SystemMetricMemoryFree       = system.MetricMemoryFree
	SystemMetricMemoryFreePC     = system.MetricMemoryFreePC
	SystemMetricMemoryTotal      = system.MetricMemoryTotal
	SystemMetricPing             = system.MetricPing
	SystemMetricRate             = system.MetricRate
	SystemMetricSession          = system.MetricSession
	SystemMetricSessionPeak      = system.MetricSessionPeak
	SystemMetricThreads          = system.MetricThreads
	SystemMetricUptime           = system.MetricUptime
	SystemMetricUsedDisk         = system.MetricUsedDisk
	SystemMetricUsedDiskPC       = system.MetricUsedDiskPC
	SystemMetricVolumeFree       = system.MetricVolumeFree
	SystemMetricVolumeFreePC     = system.MetricVolumeFreePC
	SystemMetricVolumeTotal      = system.MetricVolumeTotal

	CalcCategoryPointSnapshot   = calc.CategoryPointSnapshot
	CalcCategoryStatusMethod    = calc.CategoryStatusMethod
	CalcCategoryHistorySnapshot = calc.CategoryHistorySnapshot
	CalcCategoryHistorySeries   = calc.CategoryHistorySeries
	CalcCategoryStatistic       = calc.CategoryStatistic
	CalcCategorySystem          = calc.CategorySystem
	CalcCategoryTime            = calc.CategoryTime
	CalcCategoryWrite           = calc.CategoryWrite
	CalcCategoryPeriodic        = calc.CategoryPeriodic
	CalcCategoryMirror          = calc.CategoryMirror
	CalcCategoryWaterSteam      = calc.CategoryWaterSteam

	CalcFormulaIssueEmpty              = calc.FormulaIssueEmpty
	CalcFormulaIssueUnterminatedString = calc.FormulaIssueUnterminatedString
	CalcFormulaIssueUnterminatedBlock  = calc.FormulaIssueUnterminatedBlock
	CalcFormulaIssueReservedOP         = calc.FormulaIssueReservedOP
	CalcFormulaIssueUnknownFunction    = calc.FormulaIssueUnknownFunction

	MirrorSyncArchive     = mirror.SyncArchive
	MirrorSyncRealtime    = mirror.SyncRealtime
	MirrorSeverityError   = mirror.SeverityError
	MirrorSeverityWarning = mirror.SeverityWarning

	MutationInsert  = admin.MutationInsert
	MutationUpdate  = admin.MutationUpdate
	MutationReplace = admin.MutationReplace
	MutationDelete  = admin.MutationDelete

	ColumnNull      = admin.ColumnNull
	ColumnBool      = admin.ColumnBool
	ColumnInt8      = admin.ColumnInt8
	ColumnInt16     = admin.ColumnInt16
	ColumnInt32     = admin.ColumnInt32
	ColumnInt64     = admin.ColumnInt64
	ColumnFloat32   = admin.ColumnFloat32
	ColumnFloat64   = admin.ColumnFloat64
	ColumnDateTime  = admin.ColumnDateTime
	ColumnString    = admin.ColumnString
	ColumnBinary    = admin.ColumnBinary
	ColumnObject    = admin.ColumnObject
	ColumnMap       = admin.ColumnMap
	ColumnStructure = admin.ColumnStructure
	ColumnSlice     = admin.ColumnSlice

	FilterEQ  = admin.FilterEQ
	FilterGE  = admin.FilterGE
	FilterLE  = admin.FilterLE
	FilterIn  = admin.FilterIn
	FilterAnd = admin.FilterAnd
	FilterOr  = admin.FilterOr

	KindUnknown     = operror.KindUnknown
	KindNetwork     = operror.KindNetwork
	KindTimeout     = operror.KindTimeout
	KindCanceled    = operror.KindCanceled
	KindServer      = operror.KindServer
	KindProtocol    = operror.KindProtocol
	KindDecode      = operror.KindDecode
	KindValidation  = operror.KindValidation
	KindUnsafeSQL   = operror.KindUnsafeSQL
	KindReadOnly    = operror.KindReadOnly
	KindClosed      = operror.KindClosed
	KindUnsupported = operror.KindUnsupported

	ServerCodeUnknown1            = operror.ServerCodeUnknown1
	ServerCodeUnknown2            = operror.ServerCodeUnknown2
	ServerCodeUnknown3            = operror.ServerCodeUnknown3
	ServerCodeParameter           = operror.ServerCodeParameter
	ServerCodeFunctionUnsupported = operror.ServerCodeFunctionUnsupported
	ServerCodeMemoryReconnect     = operror.ServerCodeMemoryReconnect
	ServerCodeNetworkIOReconnect  = operror.ServerCodeNetworkIOReconnect
	ServerCodeConnectionClosed    = operror.ServerCodeConnectionClosed
	ServerCodeConnectFailed       = operror.ServerCodeConnectFailed
	ServerCodeNetworkDisconnected = operror.ServerCodeNetworkDisconnected
	ServerCodeStorageCacheWrite   = operror.ServerCodeStorageCacheWrite
	ServerCodeDataFileSizeReached = operror.ServerCodeDataFileSizeReached
	ServerCodePacketFormat        = operror.ServerCodePacketFormat
	ServerCodeCommandUnsupported  = operror.ServerCodeCommandUnsupported
	ServerCodeObjectExists        = operror.ServerCodeObjectExists
	ServerCodeObjectNotFound      = operror.ServerCodeObjectNotFound
	ServerCodeKeywordDuplicate    = operror.ServerCodeKeywordDuplicate
	ServerCodeCreateFailed        = operror.ServerCodeCreateFailed
	ServerCodeCapacityLimit       = operror.ServerCodeCapacityLimit
	ServerCodeParentNode          = operror.ServerCodeParentNode
	ServerCodeWriteDatabase       = operror.ServerCodeWriteDatabase
	ServerCodeAccessDenied        = operror.ServerCodeAccessDenied
	ServerCodeName                = operror.ServerCodeName
	ServerCodeWaitRequired        = operror.ServerCodeWaitRequired
	ServerCodeMismatch            = operror.ServerCodeMismatch
	ServerCodeParameterError      = operror.ServerCodeParameterError
	ServerCodeOutdated            = operror.ServerCodeOutdated
	ServerCodeResourceLocked      = operror.ServerCodeResourceLocked
	ServerCodeUninitialized       = operror.ServerCodeUninitialized
	ServerCodePartialError        = operror.ServerCodePartialError
	ServerCodeHistoryAccess       = operror.ServerCodeHistoryAccess
	ServerCodeInappropriatePeriod = operror.ServerCodeInappropriatePeriod
	ServerCodeNoArchive           = operror.ServerCodeNoArchive
	ServerCodeOS                  = operror.ServerCodeOS
	ServerCodeNetworkConnection   = operror.ServerCodeNetworkConnection
	ServerCodeInvalidParameter    = operror.ServerCodeInvalidParameter
	ServerCodeMemoryAllocation    = operror.ServerCodeMemoryAllocation
	ServerCodeDirectoryUse        = operror.ServerCodeDirectoryUse
	ServerCodeServiceUnknownCmd   = operror.ServerCodeServiceUnknownCmd
	ServerCodeSociInternal        = operror.ServerCodeSociInternal
	ServerCodeFileOpen            = operror.ServerCodeFileOpen
	ServerCodeFileParse           = operror.ServerCodeFileParse
	ServerCodeRange               = operror.ServerCodeRange
	ServerCodeStack               = operror.ServerCodeStack
	ServerCodeIndex               = operror.ServerCodeIndex
	ServerCodeUnknownInstruction  = operror.ServerCodeUnknownInstruction
	ServerCodeUnknownMethod       = operror.ServerCodeUnknownMethod
	ServerCodeInvalidCall         = operror.ServerCodeInvalidCall
	ServerCodeCallParameter       = operror.ServerCodeCallParameter
	ServerCodeUnknownInterface    = operror.ServerCodeUnknownInterface
	ServerCodeUnknownBuiltin      = operror.ServerCodeUnknownBuiltin
	ServerCodeVMPaused            = operror.ServerCodeVMPaused
	ServerCodeAsyncOperation      = operror.ServerCodeAsyncOperation
	ServerCodeAsyncOperationAlt   = operror.ServerCodeAsyncOperationAlt
	ServerCodeTCPConnect          = operror.ServerCodeTCPConnect
	ServerCodeTCPWrite            = operror.ServerCodeTCPWrite
	ServerCodeTCPRead             = operror.ServerCodeTCPRead
	ServerCodePacketLength        = operror.ServerCodePacketLength
	ServerCodeWaitConnection      = operror.ServerCodeWaitConnection
	ServerCodeInvalidMTable       = operror.ServerCodeInvalidMTable
	ServerCodeSystemDown          = operror.ServerCodeSystemDown
	ServerCodeDecompressData      = operror.ServerCodeDecompressData
	ServerCodeUserNotFound        = operror.ServerCodeUserNotFound
	ServerCodePasswordMismatch    = operror.ServerCodePasswordMismatch
	ServerCodePasswordMismatchAlt = operror.ServerCodePasswordMismatchAlt

	ServerCodeClassUnknown   = operror.ServerCodeClassUnknown
	ServerCodeClassTransport = operror.ServerCodeClassTransport
	ServerCodeClassProtocol  = operror.ServerCodeClassProtocol
	ServerCodeClassObject    = operror.ServerCodeClassObject
	ServerCodeClassStorage   = operror.ServerCodeClassStorage
	ServerCodeClassAccess    = operror.ServerCodeClassAccess
	ServerCodeClassHistory   = operror.ServerCodeClassHistory
	ServerCodeClassRuntime   = operror.ServerCodeClassRuntime
	ServerCodeClassScript    = operror.ServerCodeClassScript
	ServerCodeClassAuth      = operror.ServerCodeClassAuth
	ServerCodeClassDecode    = operror.ServerCodeClassDecode
)

var (
	ErrClosed         = operror.ErrClosed
	ErrInvalidOption  = operror.ErrInvalidOption
	ErrInvalidQuery   = operror.ErrInvalidQuery
	ErrUnsafeSQL      = operror.ErrUnsafeSQL
	ErrReadOnly       = operror.ErrReadOnly
	ErrNotImplemented = operror.ErrNotImplemented
)

func AX(v float32) Value { return model.AX(v) }
func DX(v bool) Value    { return model.DX(v) }
func I2(v int16) Value   { return model.I2(v) }
func I4(v int32) Value   { return model.I4(v) }
func R8(v float64) Value { return model.R8(v) }
func I8(v int64) Value   { return model.I8(v) }
func TX(v string) Value  { return model.TX(v) }
func BN(v []byte) Value  { return model.BN(v) }

func DSFromInt16(v int16) DS { return model.DSFromInt16(v) }
func BuildAlarmCode(flags ...AlarmCode) AlarmCode {
	return model.BuildAlarmCode(flags...)
}
func DefaultAlarmColors() AlarmColors {
	return model.DefaultAlarmColors()
}
func AlarmColorHex(color int32) string {
	return model.AlarmColorHex(color)
}
func ValidateAnalogAlarmLimits(code AlarmCode, limits AlarmLimits) error {
	return model.ValidateAnalogAlarmLimits(code, limits)
}
func SecurityGroupsFromBytes(v []byte) SecurityGroups {
	return model.SecurityGroupsFromBytes(v)
}
func IsErrorKind(err error, kind ErrorKind) bool {
	return operror.IsKind(err, kind)
}
func LookupServerCode(code int32) (ServerCodeInfo, bool) {
	return operror.LookupServerCode(code)
}
func ServerCodeMessage(code int32) string {
	return operror.ServerCodeMessage(code)
}
func ServerCodeRequiresReconnect(code int32) bool {
	return operror.ServerCodeRequiresReconnect(code)
}
func ServerErrorCode(err error) (int32, bool) {
	return operror.ServerErrorCode(err)
}
func IsServerCode(err error, code int32) bool {
	return operror.IsServerCode(err, code)
}
func BuildNodeInsert(db DatabaseName, nodes []Node) (TableMutation, error) {
	return admin.BuildNodeInsert(db, nodes)
}
func BuildNodeReplace(db DatabaseName, nodes []Node) (TableMutation, error) {
	return admin.BuildNodeReplace(db, nodes)
}
func BuildNodeDelete(db DatabaseName, nodes []Node) (TableMutation, error) {
	return admin.BuildNodeDelete(db, nodes)
}
func BuildPointConfigInsert(db DatabaseName, points []PointConfig) (TableMutation, error) {
	return admin.BuildPointConfigInsert(db, points)
}
func BuildPointConfigReplace(db DatabaseName, points []PointConfig) (TableMutation, error) {
	return admin.BuildPointConfigReplace(db, points)
}
func BuildPointConfigDelete(db DatabaseName, points []PointConfig) (TableMutation, error) {
	return admin.BuildPointConfigDelete(db, points)
}
func BuildReplicatorInsert(db DatabaseName, replicators []Replicator) (TableMutation, error) {
	return admin.BuildReplicatorInsert(db, replicators)
}
func BuildReplicatorReplace(db DatabaseName, replicators []Replicator) (TableMutation, error) {
	return admin.BuildReplicatorReplace(db, replicators)
}
func BuildRepItemInsert(db DatabaseName, items []RepItem) (TableMutation, error) {
	return admin.BuildRepItemInsert(db, items)
}
func BuildRepItemReplace(db DatabaseName, items []RepItem) (TableMutation, error) {
	return admin.BuildRepItemReplace(db, items)
}
func BuildUserInsert(db DatabaseName, users []UserCredential) (TableMutation, error) {
	return admin.BuildUserInsert(db, users)
}
func BuildUserReplace(db DatabaseName, users []UserCredential) (TableMutation, error) {
	return admin.BuildUserReplace(db, users)
}
func BuildUserDelete(db DatabaseName, users []User) (TableMutation, error) {
	return admin.BuildUserDelete(db, users)
}
func BuildGroupInsert(db DatabaseName, groups []Group) (TableMutation, error) {
	return admin.BuildGroupInsert(db, groups)
}
func BuildGroupReplace(db DatabaseName, groups []Group) (TableMutation, error) {
	return admin.BuildGroupReplace(db, groups)
}
func BuildAccessInsert(db DatabaseName, entries []Access) (TableMutation, error) {
	return admin.BuildAccessInsert(db, entries)
}
func BuildAccessReplace(db DatabaseName, entries []Access) (TableMutation, error) {
	return admin.BuildAccessReplace(db, entries)
}
func DefaultSystemTrendMetrics() []SystemMetric {
	return system.DefaultTrendMetrics()
}
func SystemMetrics() []SystemMetric {
	return system.Metrics()
}
func SystemCatalog(db DatabaseName) ([]SystemMetricInfo, error) {
	return system.Catalog(db)
}
func LookupSystemMetric(metric SystemMetric, db DatabaseName) (SystemMetricInfo, bool) {
	return system.LookupMetric(metric, db)
}
func SystemMetricFromGN(gn GN) (SystemMetric, bool) {
	return system.MetricFromGN(gn)
}
func SystemPointTemplates(db DatabaseName) ([]SystemPointTemplate, error) {
	return system.PointTemplates(db)
}
func LookupSystemPointTemplate(metric SystemMetric, db DatabaseName) (SystemPointTemplate, bool) {
	return system.LookupPointTemplate(metric, db)
}
func BuildSystemPointTemplateInsert(db DatabaseName, nodeID NodeID, templates []SystemPointTemplate) (TableMutation, error) {
	return system.BuildPointTemplateInsert(db, nodeID, templates)
}
func BuildSystemPointTemplateReplace(db DatabaseName, nodeID NodeID, templates []SystemPointTemplate) (TableMutation, error) {
	return system.BuildPointTemplateReplace(db, nodeID, templates)
}
func BuildDefaultSystemPointTemplateInsert(db DatabaseName, nodeID NodeID) (TableMutation, error) {
	return system.BuildDefaultPointTemplateInsert(db, nodeID)
}
func CalcFunctions() []CalcFunction {
	return calc.Functions()
}
func LookupCalcFunction(name string) (CalcFunction, bool) {
	return calc.LookupFunction(name)
}
func CalcFunctionNamesByCategory(category CalcFunctionCategory) []string {
	return calc.NamesByCategory(category)
}
func CalcFormulaReferences(formula string) []GN {
	refs := calc.FormulaReferences(formula)
	out := make([]GN, len(refs))
	for i, ref := range refs {
		out[i] = GN(ref)
	}
	return out
}
func CalcFormulaUsesFunction(formula, name string) bool {
	return calc.UsesFunction(formula, name)
}
func LintCalcFormula(formula string) []CalcFormulaIssue {
	return calc.LintFormula(formula)
}
func BuildCalcDependencyGraph(configs []PointConfig) CalcDependencyGraph {
	return calc.BuildDependencyGraph(configs)
}
func MirrorDiagnose(cfg MirrorConfig) []MirrorIssue {
	return mirror.Diagnose(cfg)
}
func MirrorSyncMonitors(points []PointConfig) []MirrorSyncMonitor {
	return mirror.SyncMonitors(points)
}
