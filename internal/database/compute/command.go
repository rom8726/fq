package compute

type CommandID int

const (
	UnknownCommandID CommandID = iota
	IncrCommandID
	GetCommandID
)

var (
	UnknownCommand = "UNKNOWN"
	IncrCommand    = "INCR"
	GetCommand     = "GET"
)

var commandNamesToID = map[string]CommandID{
	UnknownCommand: UnknownCommandID,
	IncrCommand:    IncrCommandID,
	GetCommand:     GetCommandID,
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
