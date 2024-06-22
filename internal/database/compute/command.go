package compute

type CommandID uint32

const (
	UnknownCommandID CommandID = iota
	IncrCommandID
	GetCommandID
	DelCommandID
	MsgSizeCommandID
)

var (
	UnknownCommand = "UNKNOWN"
	IncrCommand    = "INCR"
	GetCommand     = "GET"
	DelCommand     = "DEL"
	MsgSizeCommand = "MSGSIZE"
)

var commandNamesToID = map[string]CommandID{
	UnknownCommand: UnknownCommandID,
	IncrCommand:    IncrCommandID,
	GetCommand:     GetCommandID,
	DelCommand:     DelCommandID,
	MsgSizeCommand: MsgSizeCommandID,
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
