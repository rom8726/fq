package storage

import (
	"context"
	"math"
	"time"

	"fq/internal/database"
)

func (s *Storage) dumpLoop(ctx context.Context) {
	t := time.NewTicker(s.dumpInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.dump(ctx); err != nil {
				s.logger.Error().Err(err).Msg("failed to create dump")
			}
		}
	}
}

func (s *Storage) dump(ctx context.Context) error {
	dumpTx := database.Tx(s.tx.Load())
	s.dumpTx.Store(uint64(dumpTx))

	s.logger.Info().Any("dump_tx", dumpTx).Msg("Start of dump creation")

	minTx := database.Tx(math.MaxUint64)
	minTxTime := database.TxTime(time.Now().Unix())

	elemsC, errC := s.engine.Dump(ctx, dumpTx)
	for elem := range elemsC {
		if elem.Tx < minTx {
			minTx = elem.Tx
		}
		if elem.TxAt < minTxTime {
			minTxTime = elem.TxAt
		}
	}

	return <-errC
}
