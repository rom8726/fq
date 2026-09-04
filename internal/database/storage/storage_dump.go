package storage

import (
	"context"
	"errors"
	"time"

	"github.com/fq-db/fq/internal/database"
)

var errDumpDisabled = errors.New("dump is disabled")

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
	if s.dumper == nil {
		return errDumpDisabled
	}

	s.dumpOpMu.Lock()
	defer s.dumpOpMu.Unlock()

	s.mutationMu.Lock()
	dumpTx := database.Tx(s.tx.Load())
	s.dumpTx.Store(uint64(dumpTx))
	snapshot, snapshotErr := s.engine.Snapshot(ctx, dumpTx)
	s.mutationMu.Unlock()

	if snapshotErr != nil {
		return snapshotErr
	}

	start := time.Now()
	s.logger.Info().Any("dump_tx", dumpTx).Msg("Start of dump creation")
	err := s.dumper.Dump(ctx, dumpTx, snapshot)
	elapsed := time.Since(start)
	s.logger.Info().Str("elapsed", elapsed.String()).Msg("Dump creation finished")

	s.lastDump.Store(&dumpSnapshot{
		at:       start,
		duration: elapsed,
		err:      err,
		tx:       dumpTx,
	})

	return err
}
