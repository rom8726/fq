package compute

type CommandID uint32

const (
	UnknownCommandID CommandID = iota
	IncrCommandID
	GetCommandID
	DelCommandID
	HelloCommandID
	MDelCommandID
	WatchCommandID
	StreamCommandID
	PStreamCommandID
	RLimitCommandID
	RLimitSlidingWindowCommandID
	RLimitTokenBucketCommandID
	QuotaCommandID
	QuotaAcquireCommandID
	QuotaReleaseCommandID
	QuotaDeleteCommandID
	QStreamCommandID
	QPStreamCommandID
	RLimitFixedWindowCommandID
	QuotaSetCommandID
	FlushDBCommandID
	TruncateCommandID
	ScanCommandID
	PScanCommandID
	InspectCommandID
	AuthCommandID
)

var (
	UnknownCommand  = "UNKNOWN"
	IncrCommand     = "INCR"
	GetCommand      = "GET"
	DelCommand      = "DEL"
	HelloCommand    = "HELLO"
	MDelCommand     = "MDEL"
	WatchCommand    = "WATCH"
	StreamCommand   = "STREAM"
	PStreamCommand  = "PSTREAM"
	QStreamCommand  = "QSTREAM"
	QPStreamCommand = "QPSTREAM"
	RLimitCommand   = "RLIMIT"
	QuotaCommand    = "QUOTA"
	FlushDBCommand  = "FLUSHDB"
	TruncateCommand = "TRUNCATE"
	ScanCommand     = "SCAN"
	PScanCommand    = "PSCAN"
	InspectCommand  = "INSPECT"
	AuthCommand     = "AUTH"
)

var commandNamesToID = map[string]CommandID{
	UnknownCommand:  UnknownCommandID,
	IncrCommand:     IncrCommandID,
	GetCommand:      GetCommandID,
	DelCommand:      DelCommandID,
	HelloCommand:    HelloCommandID,
	MDelCommand:     MDelCommandID,
	WatchCommand:    WatchCommandID,
	StreamCommand:   StreamCommandID,
	PStreamCommand:  PStreamCommandID,
	QStreamCommand:  QStreamCommandID,
	QPStreamCommand: QPStreamCommandID,
	RLimitCommand:   RLimitCommandID,
	QuotaCommand:    QuotaCommandID,
	FlushDBCommand:  FlushDBCommandID,
	TruncateCommand: TruncateCommandID,
	ScanCommand:     ScanCommandID,
	PScanCommand:    PScanCommandID,
	InspectCommand:  InspectCommandID,
	AuthCommand:     AuthCommandID,
}

func (c CommandID) Int() int {
	return int(c)
}

func CommandNameToCommandID(command string) CommandID {
	id, found := commandNamesToID[command]
	if !found {
		return UnknownCommandID
	}

	return id
}
