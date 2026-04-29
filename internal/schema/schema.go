package schema

const (
	TableProduct    = "Product"
	TableRoot       = "Root"
	TableServer     = "Server"
	TableDatabase   = "Database"
	TableDAS        = "DAS"
	TableDevice     = "Device"
	TableNode       = "Node"
	TablePoint      = "Point"
	TableRealtime   = "Realtime"
	TableArchive    = "Archive"
	TableStat       = "Stat"
	TableAlarm      = "Alarm"
	TableAAlarm     = "AAlarm"
	TableUser       = "User"
	TableGroups     = "Groups"
	TableAccess     = "Access"
	TableReplicator = "Replicator"
	TableRepItem    = "RepItem"
)

const (
	ProtocolMagic    int32 = 0x10203040
	DefaultChunkSize       = 200
)
