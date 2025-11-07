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
)

var (
	UnknownCommand = "UNKNOWN"
	IncrCommand    = "INCR"
	GetCommand     = "GET"
	DelCommand     = "DEL"
	MsgSizeCommand = "MSGSIZE"
	MDelCommand    = "MDEL"
	WatchCommand   = "WATCH"
)

var commandNamesToID = map[string]CommandID{
	UnknownCommand: UnknownCommandID,
	IncrCommand:    IncrCommandID,
	GetCommand:     GetCommandID,
	DelCommand:     DelCommandID,
	MsgSizeCommand: MsgSizeCommandID,
	MDelCommand:    MDelCommandID,
	WatchCommand:   WatchCommandID,
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
