package model

import "time"

type Database struct {
	ID          int32
	UUID        UUID
	Name        string
	Description string
	DataLimit   int32
	SaveLimit   int32
	PointSize   int32
	TimeBase    int32
	Period      int32
	FileSize    int32
	IndexTable  int32
	IndexLimit  int32
	Auto        bool
	Lazy        bool
	MemoryMode  bool
	HistoryPath string
	ConfigTime  time.Time
	GN          GN
	UpdateTime  time.Time
	Status      int16
	Value       int32
}

type Product struct {
	Project     string
	Host        string
	Name        string
	Description string
	Version     string
	License     string
	Size        int32
	ExpireTime  time.Time
	Authority   string
}

type Root struct {
	ID           int32
	Name         string
	Description  string
	IP           string
	Port         int32
	IOTimeout    int32
	WriteTimeout int32
	MaxThreads   int32
	LogLevel     int32
	SyncMode     int32
	TimeDiff     int32
	StorageDir   string
	ConfigTime   time.Time
	GN           GN
	UpdateTime   time.Time
	Status       int16
	Value        int32
}

type Server struct {
	ID          int32
	Name        string
	Description string
	IP          string
	Port        int32
}

type Node struct {
	ID          NodeID
	UUID        UUID
	ParentID    NodeID
	Name        string
	Description string
	Resolution  int32
	AlarmCode   AlarmCode
	Archived    bool
	Offline     bool
	Internal    bool
	ConfigTime  time.Time
	GN          GN
}

type DAS struct {
	ID          DASID
	UUID        UUID
	NodeID      NodeID
	Name        string
	Description string
	IP          string
	Port        int32
	Version     int32
	ConfigTime  time.Time
	GN          GN
	UpdateTime  time.Time
	Status      int16
	Value       int32
}

type Device struct {
	ID          DeviceID
	UUID        UUID
	NodeID      NodeID
	DASID       DASID
	Name        string
	Description string
	Channel     int32
	IP          string
	Address     string
	LineName    string
	ConfigTime  time.Time
	GN          GN
	UpdateTime  time.Time
	Status      int16
	Value       int32
}

type User struct {
	Name string
}

type Group struct {
	ID   int32
	Name string
}

type Access struct {
	User      string
	Group     string
	Privilege string
}

type Replicator struct {
	Name            string
	IP              string
	Port            int32
	SourcePort      int32
	SyncMode        ReplicationSyncMode
	FilterUnchanged bool
	ArchiveBackfill bool
	TimeLimitDays   int32
}

type RepItem struct {
	PointName  string
	TargetName string
	Transform  ReplicationTransform
}

type Point struct {
	ID          PointID
	UUID        UUID
	NodeID      NodeID
	Source      PointSource
	Type        PointType
	Name        string
	Alias       string
	Description string
	Keyword     string
	Security    SecurityGroups
	Resolution  int16
	AlarmCode   AlarmCode
	AlarmLevel  AlarmPriority
	Archived    bool
	Unit        string
	Format      int16
	ConfigTime  time.Time
	Expression  string
	GN          GN
}

type Alarm struct {
	ID          PointID
	GN          GN
	Type        PointType
	Name        string
	Alias       string
	Description string
	Unit        string
	Level       int8
	Color       int32
	Priority    AlarmPriority
	ConfigCode  AlarmCode
	Colors      AlarmColors
	ActiveCode  AlarmCode
	FirstTime   time.Time
	AlarmTime   time.Time
	UpdateTime  time.Time
	Status      DS
	Value       Value
}

func (a Alarm) ActiveAlarm() AlarmCode {
	if a.ActiveCode != AlarmNone {
		return a.ActiveCode
	}
	return a.Status.ActiveAlarm(a.Type, a.ConfigCode)
}

func (a Alarm) DisplayColor() (int32, bool) {
	if a.Color != 0 {
		return a.Color, true
	}
	return a.Colors.Color(a.ActiveAlarm())
}

func (a Alarm) DisplayLabel() string {
	code := a.ActiveAlarm()
	if code == AlarmNone && a.Status.AlarmState() == AlarmStateRestoredUnacked {
		return "Restore alarm"
	}
	return code.LabelForPointType(a.Type)
}
