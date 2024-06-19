package database

import (
	"context"
	"errors"
	"strconv"

	"github.com/rs/zerolog"

	"fq/internal/database/compute"
)

var (
	errInternalConfiguration = errors.New("internal configuration error")
	errInvalidBatchSize      = errors.New("batch is not a number")
)

type computeLayer interface {
	HandleQuery(context.Context, string) (compute.Query, error)
}

type storageLayer interface {
	Incr(ctx context.Context, key BatchKey) (ValueType, error)
	Get(ctx context.Context, key BatchKey) (ValueType, error)
}

type Database struct {
	computeLayer computeLayer
	storageLayer storageLayer
	logger       *zerolog.Logger
}

func NewDatabase(computeLayer computeLayer, storageLayer storageLayer, logger *zerolog.Logger) *Database {
	return &Database{
		computeLayer: computeLayer,
		storageLayer: storageLayer,
		logger:       logger,
	}
}

func (d *Database) HandleQuery(ctx context.Context, queryStr string) string {
	if d.logger.GetLevel() == zerolog.DebugLevel {
		d.logger.Debug().
			Str("query", queryStr).
			Msg("handling query")
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

func makeBatchKey(key, batchSizeStr string) (BatchKey, error) {
	batchSize, err := strconv.ParseUint(batchSizeStr, 10, 32)
	if err != nil {
		return BatchKey{}, errInvalidBatchSize
	}

	return BatchKey{
		BatchSize:    uint32(batchSize),
		BatchSizeStr: batchSizeStr,
		Key:          key,
	}, nil
}

func makeErrorMsg(err error) string {
	return "[error] " + err.Error()
}

func makeValueMsg(v ValueType) string {
	return "[ok] " + strconv.FormatUint(uint64(v), 10)
}
