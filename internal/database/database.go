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
	"github.com/fq-db/fq/internal/security"
)

const (
	maxKeyLength = 1024
	maxBatchSize = math.MaxUint32
	minBatchSize = 1
	maxLimit     = uint64(1<<31 - 1)
	minLimit     = 1
	maxScanCount = uint64(10000)

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
	errInvalidScanCount      = errors.New("invalid scan count")

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
	SubscribeQuotaEvents(ctx context.Context, prefix string) (<-chan QuotaEvent, func())
	RLimitFixedWindow(ctx context.Context, key BatchKey, limit ValueType) (RateLimitResult, error)
	RLimitSlidingWindow(ctx context.Context, key BatchKey, limit ValueType) (RateLimitResult, error)
	RLimitTokenBucket(
		ctx context.Context,
		key BatchKey,
		capacity, refillAmount ValueType,
	) (RateLimitResult, error)
	QuotaAcquire(ctx context.Context, request QuotaAcquireRequest) (QuotaAcquireResult, error)
	QuotaSet(ctx context.Context, request QuotaSetRequest) (bool, error)
	QuotaRelease(ctx context.Context, name string, clientID string) (bool, error)
	QuotaDelete(ctx context.Context, name string) (bool, error)
	QuotaInfo(ctx context.Context, name string) (QuotaInfo, error)
	FlushDB(ctx context.Context) error
	Truncate(ctx context.Context) error
	Scan(ctx context.Context, prefix, cursor string, count uint32) (ScanResult, error)
}

type inspector interface {
	Report(ctx context.Context, section string) ([]byte, error)
}

type Database struct {
	computeLayer   computeLayer
	storageLayer   storageLayer
	logger         *zerolog.Logger
	maxMessageSize int
	inspector      inspector
}

func (d *Database) SetInspector(i inspector) {
	d.inspector = i
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
			Str("query", redactQuery(queryStr)).
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

	session := security.SessionFrom(ctx)

	if query.CommandID() == compute.AuthCommandID {
		authResponse, authErr := d.handleAuthQuery(session, query, responseBuffer.buf[:0])
		if authErr != nil {
			return authErr
		}

		return write(authResponse)
	}

	if err := session.Authorize(commandRole(query)); err != nil {
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
	case compute.QStreamCommandID:
		return d.handleQStreamQuery(ctx, "", write)
	case compute.QPStreamCommandID:
		return d.handleQPStreamQuery(ctx, query, write)
	case compute.RLimitCommandID:
		response = d.handleRLimitQuery(ctx, query, responseBuffer.buf[:0])
	case compute.QuotaCommandID:
		response = d.handleQuotaQuery(ctx, query, responseBuffer.buf[:0])
	case compute.FlushDBCommandID:
		response = d.handleFlushDBQuery(ctx, responseBuffer.buf[:0])
	case compute.TruncateCommandID:
		response = d.handleTruncateQuery(ctx, responseBuffer.buf[:0])
	case compute.ScanCommandID:
		response = d.handleScanQuery(ctx, query, responseBuffer.buf[:0])
	case compute.PScanCommandID:
		response = d.handlePScanQuery(ctx, query, responseBuffer.buf[:0])
	case compute.InspectCommandID:
		return d.handleInspectQuery(ctx, query, write)
	default:
		d.logger.Error().Msg("compute layer is incorrect")

		response = appendErrorMsg(responseBuffer.buf[:0], errInternalConfiguration)
	}

	return write(response)
}

func (d *Database) handleFlushDBQuery(ctx context.Context, dst []byte) []byte {
	if err := d.storageLayer.FlushDB(ctx); err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendValueMsg(dst, 1)
}

func (d *Database) handleTruncateQuery(ctx context.Context, dst []byte) []byte {
	if err := d.storageLayer.Truncate(ctx); err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendValueMsg(dst, 1)
}

func (d *Database) handleScanQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	return d.handleScan(ctx, "", query.Arg(0), query.Arg(1), dst)
}

func (d *Database) handlePScanQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	prefix, err := makeStreamPrefix(query.Arg(0))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return d.handleScan(ctx, prefix, query.Arg(1), query.Arg(2), dst)
}

func (d *Database) handleScan(ctx context.Context, prefix, cursor, countStr string, dst []byte) []byte {
	count, err := makeScanCount(countStr)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	result, err := d.storageLayer.Scan(ctx, prefix, cursor, count)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendScanMsg(dst, result)
}

func (d *Database) handleQuotaQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	action := strings.ToUpper(query.Arg(0))
	switch action {
	case "SET":
		return d.handleQuotaSetQuery(ctx, query, dst, QuotaPolicyFixed)
	case "SETN":
		return d.handleQuotaSetQuery(ctx, query, dst, QuotaPolicyPerClient)
	case "ACQ":
		return d.handleQuotaAcquireQuery(ctx, query, dst, QuotaPolicyFixed, false)
	case "ACQN":
		return d.handleQuotaAcquireQuery(ctx, query, dst, QuotaPolicyPerClient, false)
	case "ACQL":
		return d.handleQuotaAcquireQuery(ctx, query, dst, QuotaPolicyFixed, true)
	case "REL":
		return d.handleQuotaReleaseQuery(ctx, query, dst)
	case "DEL":
		return d.handleQuotaDeleteQuery(ctx, query, dst)
	case "INF":
		return d.handleQuotaInfoQuery(ctx, query, dst)
	default:
		return appendErrorMsg(dst, compute.ErrInvalidArguments)
	}
}

func (d *Database) handleQuotaSetQuery(
	ctx context.Context,
	query compute.Query,
	dst []byte,
	policy QuotaPolicy,
) []byte {
	name, err := makeQuotaName(query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}
	limit, err := makeLimit(query.Arg(2))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	clients := uint32(0)
	if policy == QuotaPolicyPerClient {
		parsedClients, parseErr := makeTTL(query.Arg(3))
		if parseErr != nil {
			return appendErrorMsg(dst, parseErr)
		}
		clients = parsedClients
	}

	changed, err := d.storageLayer.QuotaSet(ctx, QuotaSetRequest{
		Name:    name,
		Limit:   limit,
		Policy:  policy,
		Clients: clients,
	})
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendBoolsMsg(dst, []bool{changed})
}

func (d *Database) handleQuotaAcquireQuery(
	ctx context.Context,
	query compute.Query,
	dst []byte,
	policy QuotaPolicy,
	clientOwned bool,
) []byte {
	name, err := makeQuotaName(query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	limit := ValueType(0)
	amountArg := 2
	clientIDArg := 3
	ttlArg := 4
	if clientOwned {
		parsedLimit, parseErr := makeLimit(query.Arg(2))
		if parseErr != nil {
			return appendErrorMsg(dst, parseErr)
		}
		limit = parsedLimit
		amountArg = 3
		clientIDArg = 4
		ttlArg = 5
	}
	if policy == QuotaPolicyPerClient && !clientOwned {
		amountArg = -1
		clientIDArg = 2
		ttlArg = 3
	}

	amount := ValueType(0)
	if amountArg >= 0 {
		parsedAmount, parseErr := makeLimit(query.Arg(amountArg))
		if parseErr != nil {
			return appendErrorMsg(dst, parseErr)
		}
		amount = parsedAmount
	}
	clientID, err := makeQuotaClientID(query.Arg(clientIDArg))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	var ttl uint32
	if query.ArgumentCount() == ttlArg+1 {
		parsedTTL, parseErr := makeTTL(query.Arg(ttlArg))
		if parseErr != nil {
			return appendErrorMsg(dst, parseErr)
		}
		ttl = parsedTTL
	}

	result, err := d.storageLayer.QuotaAcquire(ctx, QuotaAcquireRequest{
		Name:      name,
		Limit:     limit,
		Amount:    amount,
		ClientID:  clientID,
		Ownership: quotaOwnership(clientOwned),
		Policy:    policy,
		TTL:       ttl,
	})
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendQuotaAcquireMsg(dst, result)
}

func quotaOwnership(clientOwned bool) QuotaOwnership {
	if clientOwned {
		return QuotaOwnershipClientLease
	}

	return QuotaOwnershipServer
}

func (d *Database) handleQuotaReleaseQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	name, err := makeQuotaName(query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}
	clientID, err := makeQuotaClientID(query.Arg(2))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	released, err := d.storageLayer.QuotaRelease(ctx, name, clientID)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return makeBoolMsg(released)
}

func (d *Database) handleQuotaDeleteQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	name, err := makeQuotaName(query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	deleted, err := d.storageLayer.QuotaDelete(ctx, name)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return makeBoolMsg(deleted)
}

func (d *Database) handleQuotaInfoQuery(ctx context.Context, query compute.Query, dst []byte) []byte {
	name, err := makeQuotaName(query.Arg(1))
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	info, err := d.storageLayer.QuotaInfo(ctx, name)
	if err != nil {
		return appendErrorMsg(dst, err)
	}

	return appendQuotaInfoMsg(dst, info)
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

//nolint:dupl // ok
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

func (d *Database) handleQPStreamQuery(ctx context.Context, query compute.Query, write func([]byte) error) error {
	prefix, err := makeStreamPrefix(query.Arg(0))
	if err != nil {
		return write(makeErrorMsg(err))
	}

	return d.handleQStreamQuery(ctx, prefix, write)
}

//nolint:dupl // ok
func (d *Database) handleQStreamQuery(ctx context.Context, prefix string, write func([]byte) error) error {
	events, unsubscribe := d.storageLayer.SubscribeQuotaEvents(ctx, prefix)
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

			if err := write(appendQuotaEventMsg(responseBuffer.buf[:0], event)); err != nil {
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

func makeTTL(ttlStr string) (uint32, error) {
	ttl, err := strconv.ParseUint(ttlStr, 10, 32)
	if err != nil {
		return 0, errBatchSizeNotNumber
	}
	if ttl < minBatchSize || ttl > maxBatchSize {
		return 0, fmt.Errorf(
			"%w: %d (must be between %d and %d)",
			errInvalidBatchSize,
			ttl,
			minBatchSize,
			maxBatchSize,
		)
	}

	return uint32(ttl), nil
}

func makeScanCount(countStr string) (uint32, error) {
	count, err := strconv.ParseUint(countStr, 10, 32)
	if err != nil {
		return 0, errLimitNotNumber
	}
	if count < minLimit || count > maxScanCount {
		return 0, fmt.Errorf(
			"%w: %d (must be between %d and %d)",
			errInvalidScanCount,
			count,
			minLimit,
			maxScanCount,
		)
	}

	return uint32(count), nil
}

func makeQuotaName(name string) (string, error) {
	return makeStreamPrefix(name)
}

func makeQuotaClientID(clientID string) (string, error) {
	return makeStreamPrefix(clientID)
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

func appendScanMsg(dst []byte, result ScanResult) []byte {
	dst = append(dst, "ok|"...)
	dst = append(dst, result.NextCursor...)

	for _, key := range result.Keys {
		dst = append(dst, ';')
		dst = append(dst, key.Key...)
		dst = append(dst, ';')
		dst = strconv.AppendUint(dst, uint64(key.BatchSize), 10)
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

func appendQuotaAcquireMsg(dst []byte, result QuotaAcquireResult) []byte {
	dst = append(dst, "ok|"...)
	if result.Acquired {
		dst = append(dst, '1')
	} else {
		dst = append(dst, '0')
	}

	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(result.Allocated), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(result.Used), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(result.Remaining), 10)
	dst = append(dst, ';')
	dst = strconv.AppendUint(dst, uint64(result.ExpiresAfter), 10)

	return dst
}

func appendQuotaInfoMsg(dst []byte, info QuotaInfo) []byte {
	dst = append(dst, "ok|"...)
	dst = strconv.AppendInt(dst, int64(info.Limit), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(info.Used), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(info.Remaining), 10)

	for _, client := range info.Clients {
		dst = append(dst, ';')
		dst = append(dst, client.ClientID...)
		dst = append(dst, ';')
		dst = strconv.AppendInt(dst, int64(client.Amount), 10)
		dst = append(dst, ';')
		dst = strconv.AppendUint(dst, uint64(client.ExpiresAt), 10)
	}

	return dst
}

func appendQuotaEventMsg(dst []byte, event QuotaEvent) []byte {
	dst = append(dst, "ok|"...)
	dst = append(dst, event.Event...)
	dst = append(dst, ';')
	dst = append(dst, event.Name...)
	dst = append(dst, ';')
	dst = append(dst, event.ClientID...)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(event.Amount), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(event.Used), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(event.Remaining), 10)
	dst = append(dst, ';')
	dst = strconv.AppendUint(dst, uint64(event.ExpiresAt), 10)

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
