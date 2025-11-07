package database

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rs/zerolog"

	"fq/internal/database/compute"
)

const (
	maxKeyLength = 1024
	maxBatchSize = math.MaxUint32
	minBatchSize = 1
)

var (
	errInternalConfiguration = errors.New("internal configuration error")
	errBatchSizeNotNumber    = errors.New("batch is not a number")
	errInvalidBatchSize      = errors.New("invalid batch size")
	errInvalidArgumentsCount = errors.New("invalid arguments count")
	errKeyTooLong            = errors.New("key length exceeds maximum")
	errKeyEmpty              = errors.New("key cannot be empty")
)

type computeLayer interface {
	HandleQuery(context.Context, string) (compute.Query, error)
}

type storageLayer interface {
	Incr(ctx context.Context, key BatchKey) (ValueType, error)
	Get(ctx context.Context, key BatchKey) (ValueType, error)
	Del(ctx context.Context, key BatchKey) (bool, error)
	MDel(ctx context.Context, keys []BatchKey) ([]bool, error)
	Watch(ctx context.Context, key BatchKey) (ValueType, error)
}

type Database struct {
	computeLayer   computeLayer
	storageLayer   storageLayer
	logger         *zerolog.Logger
	maxMessageSize int
}

func NewDatabase(
	computeLayer computeLayer,
	storageLayer storageLayer,
	logger *zerolog.Logger,
	maxMessageSize int,
) *Database {
	return &Database{
		computeLayer:   computeLayer,
		storageLayer:   storageLayer,
		logger:         logger,
		maxMessageSize: maxMessageSize,
	}
}

func (d *Database) HandleQuery(ctx context.Context, queryStr string) string {
	if d.logger.GetLevel() == zerolog.DebugLevel {
		d.logger.Debug().
			Str("query", queryStr).
			Msg("handling query")
	}

	// Validate message size
	if len(queryStr) > d.maxMessageSize {
		return makeErrorMsg(fmt.Errorf("message size %d exceeds maximum %d", len(queryStr), d.maxMessageSize))
	}

	query, err := d.computeLayer.HandleQuery(ctx, queryStr)
	if err != nil {
		return makeErrorMsg(err)
	}

	switch query.CommandID() {
	case compute.IncrCommandID:
		return d.handleIncrQuery(ctx, query)
	case compute.GetCommandID:
		return d.handleGetQuery(ctx, query)
	case compute.DelCommandID:
		return d.handleDelQuery(ctx, query)
	case compute.MsgSizeCommandID:
		return d.handleMsgSizeQuery()
	case compute.MDelCommandID:
		return d.handleMDelQuery(ctx, query)
	case compute.WatchCommandID:
		return d.handleWatchQuery(ctx, query)
	default:
		d.logger.Error().Msg("compute layer is incorrect")

		return makeErrorMsg(errInternalConfiguration)
	}
}

func (d *Database) handleIncrQuery(ctx context.Context, query compute.Query) string {
	arguments := query.Arguments()
	key, err := makeBatchKey(arguments[0], arguments[1])
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Incr(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeValueMsg(value)
}

func (d *Database) handleGetQuery(ctx context.Context, query compute.Query) string {
	arguments := query.Arguments()
	key, err := makeBatchKey(arguments[0], arguments[1])
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Get(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeValueMsg(value)
}

func (d *Database) handleDelQuery(ctx context.Context, query compute.Query) string {
	arguments := query.Arguments()
	key, err := makeBatchKey(arguments[0], arguments[1])
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Del(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeBoolMsg(value)
}

func (d *Database) handleMDelQuery(ctx context.Context, query compute.Query) string {
	arguments := query.Arguments()
	keys, err := makeBatchKeys(arguments)
	if err != nil {
		return makeErrorMsg(err)
	}

	values, err := d.storageLayer.MDel(ctx, keys)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeBoolsMsg(values)
}

func (d *Database) handleMsgSizeQuery() string {
	return makeValueMsg(ValueType(d.maxMessageSize))
}

func (d *Database) handleWatchQuery(ctx context.Context, query compute.Query) string {
	arguments := query.Arguments()
	key, err := makeBatchKey(arguments[0], arguments[1])
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Watch(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeValueMsg(value)
}

func makeBatchKey(key, batchSizeStr string) (BatchKey, error) {
	// Validate key
	if len(key) == 0 {
		return BatchKey{}, errKeyEmpty
	}
	if len(key) > maxKeyLength {
		return BatchKey{}, errKeyTooLong
	}

	// Validate batch size
	batchSize, err := strconv.ParseUint(batchSizeStr, 10, 64)
	if err != nil {
		return BatchKey{}, errBatchSizeNotNumber
	}

	if batchSize < minBatchSize || batchSize > maxBatchSize {
		return BatchKey{}, fmt.Errorf("%w: %d (must be between %d and %d)", errInvalidBatchSize, batchSize, minBatchSize, maxBatchSize)
	}

	return BatchKey{
		BatchSize:    uint32(batchSize),
		BatchSizeStr: batchSizeStr,
		Key:          key,
	}, nil
}

func makeBatchKeys(args []string) ([]BatchKey, error) {
	if len(args)%2 != 0 {
		return nil, errInvalidArgumentsCount
	}

	res := make([]BatchKey, 0, len(args)/2)

	for i := 0; i < len(args); i += 2 {
		key, err := makeBatchKey(args[i], args[i+1])
		if err != nil {
			return nil, err
		}

		res = append(res, key)
	}

	return res, nil
}

func makeErrorMsg(err error) string {
	return "err|" + err.Error()
}

func makeValueMsg(v ValueType) string {
	return "ok|" + strconv.FormatUint(uint64(v), 10)
}

func makeBoolMsg(v bool) string {
	var str string
	if v {
		str = "1"
	} else {
		str = "0"
	}

	return "ok|" + str
}

func makeBoolsMsg(arr []bool) string {
	var buff strings.Builder
	buff.Grow(len(arr)*2 + 3)

	buff.WriteString("ok")
	buff.WriteByte('|')

	for i, v := range arr {
		if v {
			buff.WriteString("1")
		} else {
			buff.WriteString("0")
		}

		if i < len(arr)-1 {
			buff.WriteString(";")
		}
	}

	return buff.String()
}
