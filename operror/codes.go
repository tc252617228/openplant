package operror

import "errors"

type ServerCode int32

const (
	ServerCodeUnknown1            ServerCode = -1
	ServerCodeUnknown2            ServerCode = -2
	ServerCodeUnknown3            ServerCode = -3
	ServerCodeParameter           ServerCode = -10
	ServerCodeFunctionUnsupported ServerCode = -11
	ServerCodeMemoryReconnect     ServerCode = -96
	ServerCodeNetworkIOReconnect  ServerCode = -97
	ServerCodeConnectionClosed    ServerCode = -98
	ServerCodeConnectFailed       ServerCode = -99
	ServerCodeNetworkDisconnected ServerCode = -1001
	ServerCodeStorageCacheWrite   ServerCode = -1002
	ServerCodeDataFileSizeReached ServerCode = -1003

	ServerCodePacketFormat       ServerCode = -100
	ServerCodeCommandUnsupported ServerCode = -101
	ServerCodeObjectExists       ServerCode = -102
	ServerCodeObjectNotFound     ServerCode = -103
	ServerCodeKeywordDuplicate   ServerCode = -104
	ServerCodeCreateFailed       ServerCode = -105
	ServerCodeCapacityLimit      ServerCode = -106
	ServerCodeParentNode         ServerCode = -107
	ServerCodeWriteDatabase      ServerCode = -108
	ServerCodeAccessDenied       ServerCode = -109
	ServerCodeName               ServerCode = -110
	ServerCodeWaitRequired       ServerCode = -111
	ServerCodeMismatch           ServerCode = -112
	ServerCodeParameterError     ServerCode = -113
	ServerCodeOutdated           ServerCode = -114
	ServerCodeResourceLocked     ServerCode = -115
	ServerCodeUninitialized      ServerCode = -116
	ServerCodePartialError       ServerCode = -117

	ServerCodeHistoryAccess       ServerCode = -200
	ServerCodeInappropriatePeriod ServerCode = -201
	ServerCodeNoArchive           ServerCode = -202

	ServerCodeOS                  ServerCode = 1
	ServerCodeNetworkConnection   ServerCode = 2
	ServerCodeInvalidParameter    ServerCode = 3
	ServerCodeMemoryAllocation    ServerCode = 4
	ServerCodeDirectoryUse        ServerCode = 5
	ServerCodeServiceUnknownCmd   ServerCode = 6
	ServerCodeSociInternal        ServerCode = 7
	ServerCodeFileOpen            ServerCode = 8
	ServerCodeFileParse           ServerCode = 9
	ServerCodeRange               ServerCode = 100
	ServerCodeStack               ServerCode = 101
	ServerCodeIndex               ServerCode = 102
	ServerCodeUnknownInstruction  ServerCode = 103
	ServerCodeUnknownMethod       ServerCode = 104
	ServerCodeInvalidCall         ServerCode = 105
	ServerCodeCallParameter       ServerCode = 106
	ServerCodeUnknownInterface    ServerCode = 107
	ServerCodeUnknownBuiltin      ServerCode = 108
	ServerCodeVMPaused            ServerCode = 109
	ServerCodeAsyncOperation      ServerCode = 110
	ServerCodeAsyncOperationAlt   ServerCode = 111
	ServerCodeTCPConnect          ServerCode = 200
	ServerCodeTCPWrite            ServerCode = 201
	ServerCodeTCPRead             ServerCode = 202
	ServerCodePacketLength        ServerCode = 203
	ServerCodeWaitConnection      ServerCode = 204
	ServerCodeInvalidMTable       ServerCode = 205
	ServerCodeSystemDown          ServerCode = 206
	ServerCodeDecompressData      ServerCode = 400
	ServerCodeUserNotFound        ServerCode = 410
	ServerCodePasswordMismatch    ServerCode = 411
	ServerCodePasswordMismatchAlt ServerCode = 412
)

type ServerCodeClass string

const (
	ServerCodeClassUnknown   ServerCodeClass = "unknown"
	ServerCodeClassTransport ServerCodeClass = "transport"
	ServerCodeClassProtocol  ServerCodeClass = "protocol"
	ServerCodeClassObject    ServerCodeClass = "object"
	ServerCodeClassStorage   ServerCodeClass = "storage"
	ServerCodeClassAccess    ServerCodeClass = "access"
	ServerCodeClassHistory   ServerCodeClass = "history"
	ServerCodeClassRuntime   ServerCodeClass = "runtime"
	ServerCodeClassScript    ServerCodeClass = "script"
	ServerCodeClassAuth      ServerCodeClass = "auth"
	ServerCodeClassDecode    ServerCodeClass = "decode"
)

type ServerCodeInfo struct {
	Code      ServerCode
	Name      string
	Message   string
	Class     ServerCodeClass
	Reconnect bool
}

var serverCodeInfo = map[ServerCode]ServerCodeInfo{
	ServerCodeUnknown1:            codeInfo(ServerCodeUnknown1, "Unknown1", "Unknown error", ServerCodeClassUnknown, false),
	ServerCodeUnknown2:            codeInfo(ServerCodeUnknown2, "Unknown2", "Unknown error", ServerCodeClassUnknown, false),
	ServerCodeUnknown3:            codeInfo(ServerCodeUnknown3, "Unknown3", "Unknown error", ServerCodeClassUnknown, false),
	ServerCodeParameter:           codeInfo(ServerCodeParameter, "Parameter", "Parameter error", ServerCodeClassProtocol, false),
	ServerCodeFunctionUnsupported: codeInfo(ServerCodeFunctionUnsupported, "FunctionUnsupported", "Function not supported", ServerCodeClassProtocol, false),
	ServerCodeMemoryReconnect:     codeInfo(ServerCodeMemoryReconnect, "MemoryReconnect", "Unable to allocate memory, need to reconnect", ServerCodeClassTransport, true),
	ServerCodeNetworkIOReconnect:  codeInfo(ServerCodeNetworkIOReconnect, "NetworkIOReconnect", "Network read/write IO error, need to reconnect", ServerCodeClassTransport, true),
	ServerCodeConnectionClosed:    codeInfo(ServerCodeConnectionClosed, "ConnectionClosed", "Connection closed, need to reconnect", ServerCodeClassTransport, true),
	ServerCodeConnectFailed:       codeInfo(ServerCodeConnectFailed, "ConnectFailed", "Unable to connect to server, need to reconnect", ServerCodeClassTransport, true),
	ServerCodeNetworkDisconnected: codeInfo(ServerCodeNetworkDisconnected, "NetworkDisconnected", "Network disconnect", ServerCodeClassTransport, true),
	ServerCodeStorageCacheWrite:   codeInfo(ServerCodeStorageCacheWrite, "StorageCacheWrite", "Error writing to the storage cache file", ServerCodeClassStorage, false),
	ServerCodeDataFileSizeReached: codeInfo(ServerCodeDataFileSizeReached, "DataFileSizeReached", "Data file reached configured size", ServerCodeClassStorage, false),

	ServerCodePacketFormat:       codeInfo(ServerCodePacketFormat, "PacketFormat", "Packet format error", ServerCodeClassProtocol, false),
	ServerCodeCommandUnsupported: codeInfo(ServerCodeCommandUnsupported, "CommandUnsupported", "Command not supported", ServerCodeClassProtocol, false),
	ServerCodeObjectExists:       codeInfo(ServerCodeObjectExists, "ObjectExists", "Object already exists", ServerCodeClassObject, false),
	ServerCodeObjectNotFound:     codeInfo(ServerCodeObjectNotFound, "ObjectNotFound", "Object does not exist", ServerCodeClassObject, false),
	ServerCodeKeywordDuplicate:   codeInfo(ServerCodeKeywordDuplicate, "KeywordDuplicate", "Keyword duplication", ServerCodeClassObject, false),
	ServerCodeCreateFailed:       codeInfo(ServerCodeCreateFailed, "CreateFailed", "Creation failed", ServerCodeClassObject, false),
	ServerCodeCapacityLimit:      codeInfo(ServerCodeCapacityLimit, "CapacityLimit", "System capacity limit", ServerCodeClassStorage, false),
	ServerCodeParentNode:         codeInfo(ServerCodeParentNode, "ParentNode", "Parent node error", ServerCodeClassObject, false),
	ServerCodeWriteDatabase:      codeInfo(ServerCodeWriteDatabase, "WriteDatabase", "Write database error", ServerCodeClassStorage, false),
	ServerCodeAccessDenied:       codeInfo(ServerCodeAccessDenied, "AccessDenied", "No access allowed", ServerCodeClassAccess, false),
	ServerCodeName:               codeInfo(ServerCodeName, "Name", "Name error", ServerCodeClassObject, false),
	ServerCodeWaitRequired:       codeInfo(ServerCodeWaitRequired, "WaitRequired", "Wait required", ServerCodeClassRuntime, false),
	ServerCodeMismatch:           codeInfo(ServerCodeMismatch, "Mismatch", "Mismatch", ServerCodeClassProtocol, false),
	ServerCodeParameterError:     codeInfo(ServerCodeParameterError, "ParameterError", "Parameter error", ServerCodeClassProtocol, false),
	ServerCodeOutdated:           codeInfo(ServerCodeOutdated, "Outdated", "Outdated", ServerCodeClassRuntime, false),
	// OPConsole locale files repeat ErrorCode-11 for "Resource locked"; the surrounding
	// contiguous server-code sequence indicates the intended code is -115.
	ServerCodeResourceLocked: codeInfo(ServerCodeResourceLocked, "ResourceLocked", "Resource locked", ServerCodeClassRuntime, false),
	ServerCodeUninitialized:  codeInfo(ServerCodeUninitialized, "Uninitialized", "Uninitialized", ServerCodeClassRuntime, false),
	ServerCodePartialError:   codeInfo(ServerCodePartialError, "PartialError", "Partial error", ServerCodeClassRuntime, false),

	ServerCodeHistoryAccess:       codeInfo(ServerCodeHistoryAccess, "HistoryAccess", "Access history error", ServerCodeClassHistory, false),
	ServerCodeInappropriatePeriod: codeInfo(ServerCodeInappropriatePeriod, "InappropriatePeriod", "Inappropriate interval", ServerCodeClassHistory, false),
	ServerCodeNoArchive:           codeInfo(ServerCodeNoArchive, "NoArchive", "No archiving", ServerCodeClassHistory, false),

	ServerCodeOS:                  codeInfo(ServerCodeOS, "OS", "Operating system underlying error", ServerCodeClassRuntime, false),
	ServerCodeNetworkConnection:   codeInfo(ServerCodeNetworkConnection, "NetworkConnection", "Network connection error", ServerCodeClassTransport, true),
	ServerCodeInvalidParameter:    codeInfo(ServerCodeInvalidParameter, "InvalidParameter", "Parameter error, invalid parameter used", ServerCodeClassProtocol, false),
	ServerCodeMemoryAllocation:    codeInfo(ServerCodeMemoryAllocation, "MemoryAllocation", "Memory allocation error", ServerCodeClassRuntime, false),
	ServerCodeDirectoryUse:        codeInfo(ServerCodeDirectoryUse, "DirectoryUse", "Directory usage error", ServerCodeClassStorage, false),
	ServerCodeServiceUnknownCmd:   codeInfo(ServerCodeServiceUnknownCmd, "ServiceUnknownCommand", "The current service does not know its command", ServerCodeClassProtocol, false),
	ServerCodeSociInternal:        codeInfo(ServerCodeSociInternal, "SociInternal", "Soci internal error", ServerCodeClassStorage, false),
	ServerCodeFileOpen:            codeInfo(ServerCodeFileOpen, "FileOpen", "File opening error", ServerCodeClassStorage, false),
	ServerCodeFileParse:           codeInfo(ServerCodeFileParse, "FileParse", "File content parsing error", ServerCodeClassStorage, false),
	ServerCodeRange:               codeInfo(ServerCodeRange, "Range", "Range crossed", ServerCodeClassScript, false),
	ServerCodeStack:               codeInfo(ServerCodeStack, "Stack", "Stack overrun", ServerCodeClassScript, false),
	ServerCodeIndex:               codeInfo(ServerCodeIndex, "Index", "Index out of bounds", ServerCodeClassScript, false),
	ServerCodeUnknownInstruction:  codeInfo(ServerCodeUnknownInstruction, "UnknownInstruction", "Unknown instruction", ServerCodeClassScript, false),
	ServerCodeUnknownMethod:       codeInfo(ServerCodeUnknownMethod, "UnknownMethod", "Unknown method", ServerCodeClassScript, false),
	ServerCodeInvalidCall:         codeInfo(ServerCodeInvalidCall, "InvalidCall", "Invalid call, object does not support this call", ServerCodeClassScript, false),
	ServerCodeCallParameter:       codeInfo(ServerCodeCallParameter, "CallParameter", "Call parameter error", ServerCodeClassScript, false),
	ServerCodeUnknownInterface:    codeInfo(ServerCodeUnknownInterface, "UnknownInterface", "Unknown interface", ServerCodeClassScript, false),
	ServerCodeUnknownBuiltin:      codeInfo(ServerCodeUnknownBuiltin, "UnknownBuiltin", "Unknown built-in module", ServerCodeClassScript, false),
	ServerCodeVMPaused:            codeInfo(ServerCodeVMPaused, "VMPaused", "Virtual machine execution paused", ServerCodeClassScript, false),
	ServerCodeAsyncOperation:      codeInfo(ServerCodeAsyncOperation, "AsyncOperation", "Request to perform an asynchronous operation", ServerCodeClassScript, false),
	ServerCodeAsyncOperationAlt:   codeInfo(ServerCodeAsyncOperationAlt, "AsyncOperationAlt", "Request for asynchronous operation", ServerCodeClassScript, false),
	ServerCodeTCPConnect:          codeInfo(ServerCodeTCPConnect, "TCPConnect", "Error while making TCP connection", ServerCodeClassTransport, true),
	ServerCodeTCPWrite:            codeInfo(ServerCodeTCPWrite, "TCPWrite", "TCP error writing data", ServerCodeClassTransport, true),
	ServerCodeTCPRead:             codeInfo(ServerCodeTCPRead, "TCPRead", "TCP error reading data", ServerCodeClassTransport, true),
	ServerCodePacketLength:        codeInfo(ServerCodePacketLength, "PacketLength", "Unreasonable packet length received", ServerCodeClassProtocol, false),
	ServerCodeWaitConnection:      codeInfo(ServerCodeWaitConnection, "WaitConnection", "Error waiting for connection", ServerCodeClassTransport, true),
	ServerCodeInvalidMTable:       codeInfo(ServerCodeInvalidMTable, "InvalidMTable", "Invalid MTable byte sequence", ServerCodeClassDecode, false),
	ServerCodeSystemDown:          codeInfo(ServerCodeSystemDown, "SystemDown", "System down", ServerCodeClassRuntime, false),
	ServerCodeDecompressData:      codeInfo(ServerCodeDecompressData, "DecompressData", "Incorrect decompressed data", ServerCodeClassDecode, false),
	ServerCodeUserNotFound:        codeInfo(ServerCodeUserNotFound, "UserNotFound", "User does not exist", ServerCodeClassAuth, false),
	ServerCodePasswordMismatch:    codeInfo(ServerCodePasswordMismatch, "PasswordMismatch", "Password mismatch", ServerCodeClassAuth, false),
	ServerCodePasswordMismatchAlt: codeInfo(ServerCodePasswordMismatchAlt, "PasswordMismatchAlt", "Password mismatch", ServerCodeClassAuth, false),
}

func codeInfo(code ServerCode, name, message string, class ServerCodeClass, reconnect bool) ServerCodeInfo {
	return ServerCodeInfo{
		Code:      code,
		Name:      name,
		Message:   message,
		Class:     class,
		Reconnect: reconnect,
	}
}

func LookupServerCode(code int32) (ServerCodeInfo, bool) {
	info, ok := serverCodeInfo[ServerCode(code)]
	return info, ok
}

func (c ServerCode) Info() (ServerCodeInfo, bool) {
	return LookupServerCode(int32(c))
}

func ServerCodeMessage(code int32) string {
	if info, ok := LookupServerCode(code); ok {
		return info.Message
	}
	return ""
}

func ServerCodeRequiresReconnect(code int32) bool {
	if info, ok := LookupServerCode(code); ok {
		return info.Reconnect
	}
	return false
}

func ServerErrorCode(err error) (int32, bool) {
	var e *Error
	if !errors.As(err, &e) || e.Kind != KindServer || e.Code == 0 {
		return 0, false
	}
	return e.Code, true
}

func IsServerCode(err error, code int32) bool {
	got, ok := ServerErrorCode(err)
	return ok && got == code
}
