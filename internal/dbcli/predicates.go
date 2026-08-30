package dbcli

import "strings"

const wireInspectCommand = "INSPECT"

func IsInspectCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return upperRequest == wireInspectCommand || strings.HasPrefix(upperRequest, wireInspectCommand+" ")
}

func IsHumanInspectCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return upperRequest == "HINSPECT" || strings.HasPrefix(upperRequest, "HINSPECT ")
}

func IsWatchCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return strings.HasPrefix(upperRequest, "WATCH ")
}

func IsStreamCommand(request string) bool {
	upperRequest := strings.ToUpper(strings.TrimSpace(request))
	return upperRequest == "STREAM" ||
		strings.HasPrefix(upperRequest, "PSTREAM ") ||
		upperRequest == "QSTREAM" ||
		strings.HasPrefix(upperRequest, "QPSTREAM ")
}

func IsQuitCommand(request string) bool {
	return request == "q" || request == "quit" || request == "exit"
}

func toWireInspectQuery(request string) string {
	fields := strings.Fields(request)
	if len(fields) == 0 {
		return wireInspectCommand
	}

	fields[0] = wireInspectCommand

	return strings.Join(fields, " ")
}
