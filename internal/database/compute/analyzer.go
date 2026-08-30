package compute

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

const (
	incrQueryArgumentsNumber     = 2
	getQueryArgumentsNumber      = 2
	delQueryArgumentsNumber      = 2
	msgSizeQueryArgumentsNumber  = 0
	mdelQueryArgumentsNumber     = -2
	watchQueryArgumentsNumber    = 2
	streamQueryArgumentsNumber   = 0
	pstreamQueryArgumentsNumber  = 1
	qstreamQueryArgumentsNumber  = 0
	qpstreamQueryArgumentsNumber = 1
	rlimitQueryArgumentsNumber   = -3
	quotaQueryArgumentsNumber    = -4
	flushDBQueryArgumentsNumber  = 0
	truncateQueryArgumentsNumber = 0
)

var queryArgumentsNumber = map[CommandID]int{
	IncrCommandID:     incrQueryArgumentsNumber,
	GetCommandID:      getQueryArgumentsNumber,
	DelCommandID:      delQueryArgumentsNumber,
	MsgSizeCommandID:  msgSizeQueryArgumentsNumber,
	MDelCommandID:     mdelQueryArgumentsNumber,
	WatchCommandID:    watchQueryArgumentsNumber,
	StreamCommandID:   streamQueryArgumentsNumber,
	PStreamCommandID:  pstreamQueryArgumentsNumber,
	QStreamCommandID:  qstreamQueryArgumentsNumber,
	QPStreamCommandID: qpstreamQueryArgumentsNumber,
	RLimitCommandID:   rlimitQueryArgumentsNumber,
	QuotaCommandID:    quotaQueryArgumentsNumber,
	FlushDBCommandID:  flushDBQueryArgumentsNumber,
	TruncateCommandID: truncateQueryArgumentsNumber,
}

var (
	ErrInvalidSymbol    = errors.New("invalid symbol")
	ErrInvalidCommand   = errors.New("invalid command")
	ErrInvalidArguments = errors.New("invalid arguments")
)

type Analyzer struct {
	logger *zerolog.Logger
}

func NewAnalyzer(logger *zerolog.Logger) *Analyzer {
	return &Analyzer{
		logger: logger,
	}
}

func (a *Analyzer) AnalyzeQuery(_ context.Context, tokens []string) (Query, error) {
	if len(tokens) == 0 {
		return Query{}, ErrInvalidCommand
	}

	command := strings.ToUpper(tokens[0])
	commandID := CommandNameToCommandID(command)
	if commandID == UnknownCommandID {
		return Query{}, ErrInvalidCommand
	}

	query := NewQuery(commandID, tokens[1:])
	argumentsNumber := queryArgumentsNumber[commandID]
	switch {
	case argumentsNumber >= 0:
		if len(query.Arguments()) != argumentsNumber {
			return Query{}, ErrInvalidArguments
		}
	case argumentsNumber == -2:
		if len(query.Arguments())%2 != 0 {
			return Query{}, ErrInvalidArguments
		}
	case argumentsNumber == -3:
		if !validRLimitArguments(query.Arguments()) {
			return Query{}, ErrInvalidArguments
		}
	case argumentsNumber == -4:
		if !validQuotaArguments(query.Arguments()) {
			return Query{}, ErrInvalidArguments
		}
	default:
		return Query{}, fmt.Errorf("unknown arguments count setting: %d for command %d", argumentsNumber, commandID)
	}

	if a.logger.GetLevel() == zerolog.DebugLevel {
		a.logger.Debug().Msg("query analyzed")
	}

	return query, nil
}

func validQuotaArguments(arguments []string) bool {
	if len(arguments) == 0 {
		return false
	}

	switch strings.ToUpper(arguments[0]) {
	case "SET":
		return len(arguments) == 3
	case "SETN":
		return len(arguments) == 4
	case "ACQ":
		return len(arguments) == 4 || len(arguments) == 5
	case "ACQN":
		return len(arguments) == 3 || len(arguments) == 4
	case "ACQL":
		return len(arguments) == 5 || len(arguments) == 6
	case "REL":
		return len(arguments) == 3
	case "DEL":
		return len(arguments) == 2
	case "INF":
		return len(arguments) == 2
	default:
		return false
	}
}

func validRLimitArguments(arguments []string) bool {
	if len(arguments) == 0 {
		return false
	}

	switch strings.ToUpper(arguments[0]) {
	case "FW", "SW":
		return len(arguments) == 4
	case "TB":
		return len(arguments) == 5
	default:
		return len(arguments) == 4 || len(arguments) == 5
	}
}
