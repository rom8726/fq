package compute

import (
	"context"

	"github.com/rs/zerolog"
)

type Parser struct {
	logger  *zerolog.Logger
	machine *compiledStateMachine
}

func NewParser(logger *zerolog.Logger) *Parser {
	return &Parser{
		logger:  logger,
		machine: newStateMachine(),
	}
}

func (p *Parser) ParseQuery(_ context.Context, query string) ([]string, error) {
	tokens, err := p.machine.parse(query)
	if err != nil {
		return nil, err
	}

	if p.logger.GetLevel() == zerolog.DebugLevel {
		p.logger.Debug().
			Strs("tokens", tokens).
			Msg("query parsed")
	}

	return tokens, nil
}

func (p *Parser) ParseAndAnalyzeQuery(_ context.Context, query string) (Query, error) {
	scanner := tokenScanner{query: query}
	command, ok, err := scanner.next()
	if err != nil {
		return Query{}, err
	}
	if !ok {
		return Query{}, ErrInvalidCommand
	}

	commandID := commandIDFromToken(command)
	if commandID == UnknownCommandID {
		return Query{}, ErrInvalidCommand
	}

	result, err := scanner.scanQuery(commandID)
	if err != nil {
		return Query{}, err
	}

	if p.logger.GetLevel() == zerolog.DebugLevel {
		p.logger.Debug().
			Strs("tokens", append([]string{command}, result.Arguments()...)).
			Msg("query parsed")
		p.logger.Debug().Msg("query analyzed")
	}

	return result, nil
}

func isWhiteSpace(symbol byte) bool {
	return symbol == '\t' || symbol == '\n' || symbol == ' '
}

func isLetter(symbol byte) bool {
	return (symbol >= 'a' && symbol <= 'z') ||
		(symbol >= 'A' && symbol <= 'Z') ||
		(symbol >= '0' && symbol <= '9') ||
		(symbol == '_') || (symbol == '-')
}

type tokenScanner struct {
	query string
	pos   int
}

func (s *tokenScanner) scanQuery(commandID CommandID) (Query, error) {
	switch commandID {
	case IncrCommandID, GetCommandID, DelCommandID, WatchCommandID:
		return s.scanFixedQuery(commandID, 2)
	case MsgSizeCommandID, StreamCommandID:
		return s.scanFixedQuery(commandID, 0)
	case PStreamCommandID:
		return s.scanFixedQuery(commandID, 1)
	case QStreamCommandID:
		return s.scanFixedQuery(commandID, 0)
	case QPStreamCommandID:
		return s.scanFixedQuery(commandID, 1)
	case FlushDBCommandID, TruncateCommandID:
		return s.scanFixedQuery(commandID, 0)
	case ScanCommandID:
		return s.scanFixedQuery(commandID, 2)
	case PScanCommandID:
		return s.scanFixedQuery(commandID, 3)
	case RLimitCommandID:
		return s.scanRLimitQuery()
	case QuotaCommandID:
		return s.scanQuotaQuery()
	case MDelCommandID:
		return s.scanMDelQuery()
	default:
		return Query{}, ErrInvalidCommand
	}
}

func (s *tokenScanner) scanQuotaQuery() (Query, error) {
	var args []string
	for {
		arg, ok, err := s.next()
		if err != nil {
			return Query{}, err
		}
		if !ok {
			break
		}

		args = append(args, arg)
		if len(args) > 6 {
			return Query{}, ErrInvalidArguments
		}
	}
	if !validQuotaArguments(args) {
		return Query{}, ErrInvalidArguments
	}

	return NewQuery(QuotaCommandID, args), nil
}

func (s *tokenScanner) scanFixedQuery(commandID CommandID, argCount int) (Query, error) {
	args, err := s.scanFixedArguments(argCount)
	if err != nil {
		return Query{}, err
	}
	if ok, err := s.done(); err != nil {
		return Query{}, err
	} else if !ok {
		return Query{}, ErrInvalidArguments
	}

	return NewQueryFromSlots(commandID, argCount, args[0], args[1], args[2], args[3], args[4]), nil
}

func (s *tokenScanner) scanRLimitQuery() (Query, error) {
	args, count, err := s.scanUpToArguments(5)
	if err != nil {
		return Query{}, err
	}
	if ok, err := s.done(); err != nil {
		return Query{}, err
	} else if !ok {
		return Query{}, ErrInvalidArguments
	}
	if !validRLimitArgumentsCount(args[0], count) {
		return Query{}, ErrInvalidArguments
	}

	return NewQueryFromSlots(RLimitCommandID, count, args[0], args[1], args[2], args[3], args[4]), nil
}

func (s *tokenScanner) scanMDelQuery() (Query, error) {
	var args []string
	for {
		arg, ok, err := s.next()
		if err != nil {
			return Query{}, err
		}
		if !ok {
			break
		}

		args = append(args, arg)
	}
	if len(args)%2 != 0 {
		return Query{}, ErrInvalidArguments
	}

	return NewQuery(MDelCommandID, args), nil
}

func (s *tokenScanner) scanFixedArguments(count int) ([5]string, error) {
	args, found, err := s.scanUpToArguments(count)
	if err != nil {
		return [5]string{}, err
	}
	if found != count {
		return [5]string{}, ErrInvalidArguments
	}

	return args, nil
}

func (s *tokenScanner) scanUpToArguments(maxCount int) (args [5]string, count int, err error) {
	for i := 0; i < maxCount; i++ {
		arg, ok, err := s.next()
		if err != nil {
			return [5]string{}, 0, err
		}
		if !ok {
			return args, i, nil
		}

		args[i] = arg
	}

	return args, maxCount, nil
}

func (s *tokenScanner) done() (bool, error) {
	_, ok, err := s.next()
	if err != nil {
		return false, err
	}

	return !ok, nil
}

func (s *tokenScanner) next() (token string, ok bool, err error) {
	for s.pos < len(s.query) && isWhiteSpace(s.query[s.pos]) {
		s.pos++
	}
	if s.pos == len(s.query) {
		return "", false, nil
	}

	start := s.pos
	for s.pos < len(s.query) {
		symbol := s.query[s.pos]
		switch {
		case isLetter(symbol):
			s.pos++
		case isWhiteSpace(symbol):
			return s.query[start:s.pos], true, nil
		default:
			return "", false, ErrInvalidSymbol
		}
	}

	return s.query[start:s.pos], true, nil
}

func commandIDFromToken(token string) CommandID {
	switch {
	case asciiEqualFold(token, IncrCommand):
		return IncrCommandID
	case asciiEqualFold(token, GetCommand):
		return GetCommandID
	case asciiEqualFold(token, DelCommand):
		return DelCommandID
	case asciiEqualFold(token, MsgSizeCommand):
		return MsgSizeCommandID
	case asciiEqualFold(token, MDelCommand):
		return MDelCommandID
	case asciiEqualFold(token, WatchCommand):
		return WatchCommandID
	case asciiEqualFold(token, StreamCommand):
		return StreamCommandID
	case asciiEqualFold(token, PStreamCommand):
		return PStreamCommandID
	case asciiEqualFold(token, QStreamCommand):
		return QStreamCommandID
	case asciiEqualFold(token, QPStreamCommand):
		return QPStreamCommandID
	case asciiEqualFold(token, RLimitCommand):
		return RLimitCommandID
	case asciiEqualFold(token, QuotaCommand):
		return QuotaCommandID
	case asciiEqualFold(token, FlushDBCommand):
		return FlushDBCommandID
	case asciiEqualFold(token, TruncateCommand):
		return TruncateCommandID
	case asciiEqualFold(token, ScanCommand):
		return ScanCommandID
	case asciiEqualFold(token, PScanCommand):
		return PScanCommandID
	default:
		return UnknownCommandID
	}
}

func validRLimitArgumentsCount(algorithm string, count int) bool {
	switch {
	case asciiEqualFold(algorithm, "FW"), asciiEqualFold(algorithm, "SW"):
		return count == 4
	case asciiEqualFold(algorithm, "TB"):
		return count == 5
	default:
		return count == 4 || count == 5
	}
}

func asciiEqualFold(left, right string) bool {
	if len(left) != len(right) {
		return false
	}

	for i := 0; i < len(left); i++ {
		l := left[i]
		if l >= 'a' && l <= 'z' {
			l -= 'a' - 'A'
		}
		if l != right[i] {
			return false
		}
	}

	return true
}
