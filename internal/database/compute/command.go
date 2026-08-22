package compute

type CommandID uint32

const (
	UnknownCommandID CommandID = iota
	IncrCommandID
	GetCommandID
	DelCommandID
	MsgSizeCommandID
	MDelCommandID
	WatchCommandID
	RLimitCommandID
	RLimitSlidingWindowCommandID
)

var (
	UnknownCommand = "UNKNOWN"
	IncrCommand    = "INCR"
	GetCommand     = "GET"
	DelCommand     = "DEL"
	MsgSizeCommand = "MSGSIZE"
	MDelCommand    = "MDEL"
	WatchCommand   = "WATCH"
	RLimitCommand  = "RLIMIT"
)

var commandNamesToID = map[string]CommandID{
	UnknownCommand: UnknownCommandID,
	IncrCommand:    IncrCommandID,
	GetCommand:     GetCommandID,
	DelCommand:     DelCommandID,
	MsgSizeCommand: MsgSizeCommandID,
	MDelCommand:    MDelCommandID,
	WatchCommand:   WatchCommandID,
	RLimitCommand:  RLimitCommandID,
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
