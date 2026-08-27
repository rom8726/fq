package database

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database/compute"
)

const (
	maxKeyLength = 1024
	maxBatchSize = math.MaxUint32
	minBatchSize = 1
	maxLimit     = uint64(1<<31 - 1)
	minLimit     = 1
)

var (
	errInternalConfiguration = errors.New("internal configuration error")
	errBatchSizeNotNumber    = errors.New("batch is not a number")
	errInvalidBatchSize      = errors.New("invalid batch size")
	errInvalidArgumentsCount = errors.New("invalid arguments count")
	errKeyTooLong            = errors.New("key length exceeds maximum")
	errKeyEmpty              = errors.New("key cannot be empty")
	errLimitNotNumber        = errors.New("limit is not a number")
	errInvalidLimit          = errors.New("invalid limit")
	errInvalidRLimitAlgo     = errors.New("invalid rate limit algorithm")

	okTrueMsg  = []byte("ok|1")
	okFalseMsg = []byte("ok|0")
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
	SubscribeLimitEvents(ctx context.Context, prefix string) (<-chan LimitEvent, func())
	RLimitFixedWindow(ctx context.Context, key BatchKey, limit ValueType) (RateLimitResult, error)
	RLimitSlidingWindow(ctx context.Context, key BatchKey, limit ValueType) (RateLimitResult, error)
	RLimitTokenBucket(
		ctx context.Context,
		key BatchKey,
		capacity, refillAmount ValueType,
	) (RateLimitResult, error)
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
	var response []byte
	err := d.HandleQueryStream(ctx, queryStr, func(msg []byte) error {
		response = msg

		return nil
	})
	if err != nil {
		return string(makeErrorMsg(err))
	}

	return string(response)
}

func (d *Database) HandleQueryStream(ctx context.Context, queryStr string, write func([]byte) error) error {
	if d.logger.GetLevel() == zerolog.DebugLevel {
		d.logger.Debug().
			Str("query", queryStr).
			Msg("handling query")
	}

	// Validate message size
	if len(queryStr) > d.maxMessageSize {
		return write(makeErrorMsg(fmt.Errorf("message size %d exceeds maximum %d", len(queryStr), d.maxMessageSize)))
	}

	query, err := d.computeLayer.HandleQuery(ctx, queryStr)
	if err != nil {
		return write(makeErrorMsg(err))
	}

	var response []byte
	switch query.CommandID() {
	case compute.IncrCommandID:
		response = d.handleIncrQuery(ctx, query)
	case compute.GetCommandID:
		response = d.handleGetQuery(ctx, query)
	case compute.DelCommandID:
		response = d.handleDelQuery(ctx, query)
	case compute.MsgSizeCommandID:
		response = d.handleMsgSizeQuery()
	case compute.MDelCommandID:
		response = d.handleMDelQuery(ctx, query)
	case compute.WatchCommandID:
		response = d.handleWatchQuery(ctx, query)
	case compute.StreamCommandID:
		return d.handleStreamQuery(ctx, "", write)
	case compute.PStreamCommandID:
		return d.handlePStreamQuery(ctx, query, write)
	case compute.RLimitCommandID:
		response = d.handleRLimitQuery(ctx, query)
	default:
		d.logger.Error().Msg("compute layer is incorrect")

		response = makeErrorMsg(errInternalConfiguration)
	}

	return write(response)
}

func (d *Database) handleIncrQuery(ctx context.Context, query compute.Query) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Incr(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeValueMsg(value)
}

func (d *Database) handleGetQuery(ctx context.Context, query compute.Query) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Get(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeValueMsg(value)
}

func (d *Database) handleDelQuery(ctx context.Context, query compute.Query) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Del(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeBoolMsg(value)
}

func (d *Database) handleMDelQuery(ctx context.Context, query compute.Query) []byte {
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

func (d *Database) handleMsgSizeQuery() []byte {
	return makeValueMsg(ValueType(d.maxMessageSize))
}

func (d *Database) handleWatchQuery(ctx context.Context, query compute.Query) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return makeErrorMsg(err)
	}

	value, err := d.storageLayer.Watch(ctx, key)
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeValueMsg(value)
}

func (d *Database) handlePStreamQuery(ctx context.Context, query compute.Query, write func([]byte) error) error {
	prefix, err := makeStreamPrefix(query.Arg(0))
	if err != nil {
		return write(makeErrorMsg(err))
	}

	return d.handleStreamQuery(ctx, prefix, write)
}

func (d *Database) handleStreamQuery(ctx context.Context, prefix string, write func([]byte) error) error {
	events, unsubscribe := d.storageLayer.SubscribeLimitEvents(ctx, prefix)
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return ctx.Err()
			}

			if err := write(makeLimitEventMsg(event)); err != nil {
				return err
			}
		}
	}
}

func makeStreamPrefix(prefix string) (string, error) {
	if prefix == "" {
		return "", errKeyEmpty
	}
	if len(prefix) > maxKeyLength {
		return "", errKeyTooLong
	}

	return prefix, nil
}

func (d *Database) handleRLimitQuery(ctx context.Context, query compute.Query) []byte {
	algorithm := strings.ToUpper(query.Arg(0))
	if algorithm != "FW" && algorithm != "SW" && algorithm != "TB" {
		return makeErrorMsg(errInvalidRLimitAlgo)
	}

	windowArgIndex := 3
	if algorithm == "TB" {
		windowArgIndex = 4
	}
	key, err := makeBatchKey(query.Arg(1), query.Arg(windowArgIndex))
	if err != nil {
		return makeErrorMsg(err)
	}

	limit, err := makeLimit(query.Arg(2))
	if err != nil {
		return makeErrorMsg(err)
	}

	var result RateLimitResult
	switch algorithm {
	case "FW":
		result, err = d.storageLayer.RLimitFixedWindow(ctx, key, limit)
	case "SW":
		result, err = d.storageLayer.RLimitSlidingWindow(ctx, key, limit)
	case "TB":
		refillAmount, parseErr := makeLimit(query.Arg(3))
		if parseErr != nil {
			return makeErrorMsg(parseErr)
		}

		result, err = d.storageLayer.RLimitTokenBucket(ctx, key, limit, refillAmount)
	}
	if err != nil {
		return makeErrorMsg(err)
	}

	return makeRateLimitMsg(result)
}

func makeBatchKey(key, batchSizeStr string) (BatchKey, error) {
	// Validate key
	if key == "" {
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
		return BatchKey{}, fmt.Errorf(
			"%w: %d (must be between %d and %d)",
			errInvalidBatchSize,
			batchSize,
			minBatchSize,
			maxBatchSize,
		)
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

func makeLimit(limitStr string) (ValueType, error) {
	limit, err := strconv.ParseUint(limitStr, 10, 64)
	if err != nil {
		return 0, errLimitNotNumber
	}

	if limit < minLimit || limit > maxLimit {
		return 0, fmt.Errorf(
			"%w: %d (must be between %d and %d)",
			errInvalidLimit,
			limit,
			minLimit,
			maxLimit,
		)
	}

	return ValueType(limit), nil
}

func makeErrorMsg(err error) []byte {
	buf := makeResponseBuffer(len("err|") + len(err.Error()))
	buf = append(buf, "err|"...)
	buf = append(buf, err.Error()...)

	return buf
}

func makeValueMsg(v ValueType) []byte {
	buf := makeResponseBuffer(len("ok|18446744073709551615"))
	buf = append(buf, "ok|"...)
	buf = strconv.AppendUint(buf, uint64(v), 10)

	return buf
}

func makeBoolMsg(v bool) []byte {
	if v {
		return okTrueMsg
	}

	return okFalseMsg
}

func makeBoolsMsg(arr []bool) []byte {
	buf := makeResponseBuffer(len(arr)*2 + 3)
	buf = append(buf, "ok|"...)

	for i, v := range arr {
		if v {
			buf = append(buf, '1')
		} else {
			buf = append(buf, '0')
		}

		if i < len(arr)-1 {
			buf = append(buf, ';')
		}
	}

	return buf
}

func makeRateLimitMsg(result RateLimitResult) []byte {
	buf := makeResponseBuffer(len("ok|1;-2147483648;-2147483648;4294967295"))
	buf = append(buf, "ok|"...)
	if result.Allowed {
		buf = append(buf, '1')
	} else {
		buf = append(buf, '0')
	}

	buf = append(buf, ';')
	buf = strconv.AppendInt(buf, int64(result.Current), 10)
	buf = append(buf, ';')
	buf = strconv.AppendInt(buf, int64(result.Remaining), 10)
	buf = append(buf, ';')
	buf = strconv.AppendUint(buf, uint64(result.ResetAfter), 10)

	return buf
}

func makeLimitEventMsg(event LimitEvent) []byte {
	buf := makeResponseBuffer(len(event.Key) + len("ok|;-2147483648;4294967295;4294967295"))
	buf = append(buf, "ok|"...)
	buf = append(buf, event.Key...)
	buf = append(buf, ';')
	buf = strconv.AppendUint(buf, uint64(event.Window), 10)
	buf = append(buf, ';')
	buf = strconv.AppendInt(buf, int64(event.Current), 10)
	buf = append(buf, ';')
	buf = strconv.AppendUint(buf, uint64(event.ResetAfter), 10)

	return buf
}

func makeResponseBuffer(capacity int) []byte {
	return make([]byte, 0, capacity)
}
