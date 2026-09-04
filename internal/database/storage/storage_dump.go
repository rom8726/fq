package storage

import (
	"context"
	"errors"
	"time"

	"github.com/fq-db/fq/internal/database"
)

var errDumpDisabled = errors.New("dump is disabled")

func (s *Storage) dumpLoop(ctx context.Context) {
	if !s.waitForDumpBootstrap(ctx) {
		return
	}

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

func (s *Storage) waitForDumpBootstrap(ctx context.Context) bool {
	if s.replica == nil || s.replica.IsMaster() {
		return true
	}

	waiter, ok := s.replica.(dumpBootstrapWaiter)
	if !ok {
		s.logger.Warn().Msg("periodic dump is disabled: replica cannot report replication bootstrap")

		return false
	}

	if err := waiter.WaitDumpApplied(ctx); err != nil {
		s.logger.Info().Err(err).Msg("periodic dump stopped while waiting for replication bootstrap")

		return false
	}

	return true
}

func (s *Storage) dump(ctx context.Context) error {
	if s.dumper == nil {
		return errDumpDisabled
	}

	s.dumpOpMu.Lock()
	defer s.dumpOpMu.Unlock()

	s.mutationMu.Lock()
	dumpTx, snapshot, snapshotErr := s.takeDumpSnapshot(ctx)
	if snapshotErr == nil {
		s.dumpTx.Store(uint64(dumpTx))
	}
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

func (s *Storage) takeDumpSnapshot(ctx context.Context) (database.Tx, database.DumpSnapshot, error) {
	if s.replica != nil && !s.replica.IsMaster() {
		snapshot, appliedTx, err := s.engine.SnapshotApplied(ctx)

		return appliedTx, snapshot, err
	}

	dumpTx := database.Tx(s.tx.Load())
	snapshot, err := s.engine.Snapshot(ctx, dumpTx)

	return dumpTx, snapshot, err
}
