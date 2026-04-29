package protocol

const (
	Magic int32 = 0x10203040
)

type Command int32

const (
	CommandSelect  Command = 110
	CommandUpdate  Command = 120
	CommandInsert  Command = 130
	CommandDelete  Command = 140
	CommandReplace Command = 150
)

type URL int32

const (
	URLScheme       URL = 0x20000000
	URLID           URL = 0x21000000
	URLStatic       URL = 0x22000000
	URLDynamic      URL = 0x23000000
	URLChildID      URL = 0x24000000
	URLChildStatic  URL = 0x25000000
	URLChildDynamic URL = 0x26000000
	URLAlarm        URL = 0x2A000000
	URLChildAlarm   URL = 0x2B000000
	URLArchive      URL = 0x30000000
	URLCloudNodes   URL = 0x40000000
	URLCloudNode    URL = 0x41000000
	URLCloudDBs     URL = 0x42000000
	URLCloudDB      URL = 0x43000000
	URLCloudTime    URL = 0x44000000
	URLEcho         URL = 0x46000000
)

const (
	FlagByName   int16 = 1
	FlagByID     int16 = 2
	FlagFilter   int16 = 4
	FlagNoDS     int16 = 0x40
	FlagNoTM     int16 = 0x80
	FlagWall     int16 = 0x100
	FlagMMI      int16 = 0x200
	FlagSync     int16 = 0x400
	FlagCtrl     int16 = 0x800
	FlagFeedback int16 = 0x1000
	FlagCache    int16 = 0x2000
)

const (
	PropReqID     = "Reqid"
	PropService   = "Service"
	PropTable     = "Table"
	PropAction    = "Action"
	PropSubject   = "Subject"
	PropOption    = "Option"
	PropOrderBy   = "OrderBy"
	PropLimit     = "Limit"
	PropAsync     = "Async"
	PropColumns   = "Columns"
	PropKey       = "Key"
	PropIndexes   = "Indexes"
	PropFilters   = "Filters"
	PropError     = "Error"
	PropErrNo     = "Errno"
	PropSQL       = "SQL"
	PropToken     = "Token"
	PropDB        = "db"
	PropTimestamp = "Time"
	PropSnapshot  = "Snapshot"
	PropSubscribe = "Subscribe"
)

const (
	ActionCreate  = "Create"
	ActionSelect  = "Select"
	ActionInsert  = "Insert"
	ActionUpdate  = "Update"
	ActionReplace = "Replace"
	ActionDelete  = "Delete"
	ActionExecSQL = "ExecSQL"
	ActionCommit  = "Commit"
)
