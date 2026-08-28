package database

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database/compute"
)

const (
	maxKeyLength = 1024
	maxBatchSize = math.MaxUint32
	minBatchSize = 1
	maxLimit     = uint64(1<<31 - 1)
	minLimit     = 1

	defaultResponseBufferCapacity = 64
	maxPooledResponseBufferSize   = 64 << 10
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

	responseBufferPool = sync.Pool{
		New: func() any {
			return &responseBuffer{
				buf: make([]byte, 0, defaultResponseBufferCapacity),
			}
		},
	}
)

type responseBuffer struct {
	buf []byte
}

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
	var response string
	err := d.HandleQueryStream(ctx, queryStr, func(msg []byte) error {
		response = string(msg)

		return nil
	})
	if err != nil {
		return string(makeErrorMsg(err))
	}

	return response
}

func (d *Database) HandleQueryStream(ctx context.Context, queryStr string, write func([]byte) error) error {
	if d.logger.GetLevel() == zerolog.DebugLevel {
		d.logger.Debug().
			Str("query", queryStr).
			Msg("handling query")
	}

	responseBuffer := acquireResponseBuffer()
	defer releaseResponseBuffer(responseBuffer)

	// Validate message size
	if len(queryStr) > d.maxMessageSize {
		response := appendErrorMsg(
			responseBuffer.buf[:0],
			fmt.Errorf("message size %d exceeds maximum %d", len(queryStr), d.maxMessageSize),
		)

		return write(response)
	}

	query, err := d.computeLayer.HandleQuery(ctx, queryStr)
	if err != nil {
		return write(appendErrorMsg(responseBuffer.buf[:0], err))
	}

	var response []byte
	switch query.CommandID() {
	case compute.IncrCommandID:
		response = d.handleIncrQuery(ctx, query, responseBuffer.buf[:0])
	case compute.GetCommandID:
		response = d.handleGetQuery(ctx, query, responseBuffer.buf[:0])
	case compute.DelCommandID:
		response = d.handleDelQuery(ctx, query, responseBuffer.buf[:0])
	case compute.MsgSizeCommandID:
		response = d.handleMsgSizeQuery(responseBuffer.buf[:0])
	case compute.MDelCommandID:
		response = d.handleMDelQuery(ctx, query, responseBuffer.buf[:0])
	case compute.WatchCommandID:
		response = d.handleWatchQuery(ctx, query, responseBuffer.buf[:0])
	case compute.StreamCommandID:
		return d.handleStreamQuery(ctx, "", write)
	case compute.PStreamCommandID:
		return d.handlePStreamQuery(ctx, query, write)
	case compute.RLimitCommandID:
		response = d.handleRLimitQuery(ctx, query, responseBuffer.buf[:0])
	default:
		d.logger.Error().Msg("compute layer is incorrect")

		response = appendErrorMsg(responseBuffer.buf[:0], errInternalConfiguration)
	}

	return write(response)
}

func (d *Database) handleIncrQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	value, err := d.storageLayer.Incr(ctx, key)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendValueMsg(dst, value)
}

func (d *Database) handleGetQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	value, err := d.storageLayer.Get(ctx, key)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendValueMsg(dst, value)
}

func (d *Database) handleDelQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	value, err := d.storageLayer.Del(ctx, key)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return makeBoolMsg(value)
}

func (d *Database) handleMDelQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	arguments := query.Arguments()
	keys, err := makeBatchKeys(arguments)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	values, err := d.storageLayer.MDel(ctx, keys)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendBoolsMsg(dst, values)
}

func (d *Database) handleMsgSizeQuery(dst []byte) []byte {
	return appendValueMsg(dst, ValueType(d.maxMessageSize))
}

func (d *Database) handleWatchQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	key, err := makeBatchKey(query.Arg(0), query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	value, err := d.storageLayer.Watch(ctx, key)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendValueMsg(dst, value)
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
	responseBuffer := acquireResponseBuffer()
	defer releaseResponseBuffer(responseBuffer)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return ctx.Err()
			}

			if err := write(appendLimitEventMsg(responseBuffer.buf[:0], event)); err != nil {
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

func (d *Database) handleRLimitQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	algorithm := strings.ToUpper(query.Arg(0))
	if algorithm != "FW" && algorithm != "SW" && algorithm != "TB" {
		return appendErrorMsg(dst, errInvalidRLimitAlgo)
	}

	windowArgIndex := 3
	if algorithm == "TB" {
		windowArgIndex = 4
	}
	key, err := makeBatchKey(query.Arg(1), query.Arg(windowArgIndex))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	limit, err := makeLimit(query.Arg(2))
	if err != nil {
		return appendErrorMsg(dst, err)
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
			return appendErrorMsg(dst, parseErr)
		}

		result, err = d.storageLayer.RLimitTokenBucket(ctx, key, limit, refillAmount)
	}
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendRateLimitMsg(dst, result)
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
	return appendErrorMsg(makeResponseBuffer(len("err|")+len(err.Error())), err)
}

func makeBoolMsg(v bool) []byte {
	if v {
		return okTrueMsg
	}

	return okFalseMsg
}

func makeResponseBuffer(capacity int) []byte {
	return make([]byte, 0, capacity)
}

func appendErrorMsg(dst []byte, err error) []byte {
	dst = append(dst, "err|"...)
	dst = append(dst, err.Error()...)

	return dst
}

func appendValueMsg(dst []byte, v ValueType) []byte {
	dst = append(dst, "ok|"...)
	dst = strconv.AppendUint(dst, uint64(v), 10)

	return dst
}

func appendBoolsMsg(dst []byte, arr []bool) []byte {
	dst = append(dst, "ok|"...)

	for i, v := range arr {
		if v {
			dst = append(dst, '1')
		} else {
			dst = append(dst, '0')
		}

		if i < len(arr)-1 {
			dst = append(dst, ';')
		}
	}

	return dst
}

func appendRateLimitMsg(dst []byte, result RateLimitResult) []byte {
	dst = append(dst, "ok|"...)
	if result.Allowed {
		dst = append(dst, '1')
	} else {
		dst = append(dst, '0')
	}

	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(result.Current), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(result.Remaining), 10)
	dst = append(dst, ';')
	dst = strconv.AppendUint(dst, uint64(result.ResetAfter), 10)

	return dst
}

func appendLimitEventMsg(dst []byte, event LimitEvent) []byte {
	dst = append(dst, "ok|"...)
	dst = append(dst, event.Key...)
	dst = append(dst, ';')
	dst = strconv.AppendUint(dst, uint64(event.Window), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(event.Current), 10)
	dst = append(dst, ';')
	dst = strconv.AppendUint(dst, uint64(event.ResetAfter), 10)

	return dst
}

func acquireResponseBuffer() *responseBuffer {
	return responseBufferPool.Get().(*responseBuffer)
}

func releaseResponseBuffer(buffer *responseBuffer) {
	if cap(buffer.buf) > maxPooledResponseBufferSize {
		return
	}

	buffer.buf = buffer.buf[:0]
	responseBufferPool.Put(buffer)
}
