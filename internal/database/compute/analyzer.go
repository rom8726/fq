package compute

import (
	"context"
	"errors"
	"strings"

	"github.com/rs/zerolog"
)

const (
	incrQueryArgumentsNumber = 2
	getQueryArgumentsNumber  = 2
)

var queryArgumentsNumber = map[CommandID]int{
	IncrCommandID: incrQueryArgumentsNumber,
	GetCommandID:  getQueryArgumentsNumber,
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
	if len(query.Arguments()) != argumentsNumber {
		return Query{}, ErrInvalidArguments
	}

	if a.logger.GetLevel() == zerolog.DebugLevel {
		a.logger.Debug().Msg("query analyzed")
	}

	return query, nil
}
