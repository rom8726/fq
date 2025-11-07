package compute

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

const (
	incrQueryArgumentsNumber    = 2
	getQueryArgumentsNumber     = 2
	delQueryArgumentsNumber     = 2
	msgSizeQueryArgumentsNumber = 0
	mdelQueryArgumentsNumber    = -2
	watchQueryArgumentsNumber   = 2
)

var queryArgumentsNumber = map[CommandID]int{
	IncrCommandID:    incrQueryArgumentsNumber,
	GetCommandID:     getQueryArgumentsNumber,
	DelCommandID:     delQueryArgumentsNumber,
	MsgSizeCommandID: msgSizeQueryArgumentsNumber,
	MDelCommandID:    mdelQueryArgumentsNumber,
	WatchCommandID:   watchQueryArgumentsNumber,
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
	default:
		return Query{}, fmt.Errorf("unknown arguments count setting: %d for command %d", argumentsNumber, commandID)
	}

	if a.logger.GetLevel() == zerolog.DebugLevel {
		a.logger.Debug().Msg("query analyzed")
	}

	return query, nil
}
