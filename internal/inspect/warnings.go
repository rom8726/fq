package inspect

import (
	"fmt"
	"time"

	"github.com/fq-db/fq/internal/config"
)

const (
	walQueuePressureThreshold  = 0.75
	replicaLagWarnThresholdSec = 5.0
	dumpOverdueFactor          = 2

	severityWarn = "warn"
	severityCrit = "crit"
)

func buildWarnings(report *Report) []Warning {
	warnings := make([]Warning, 0, 4)

	warnings = append(warnings, walQueueWarnings(report.WAL)...)
	warnings = append(warnings, replWarnings(report.Repl)...)
	warnings = append(warnings, streamWarnings(report.Streams)...)
	warnings = append(warnings, dumpWarnings(report.Dump)...)
	warnings = append(warnings, durabilityWarnings(report.Persistence, report.WAL)...)

	return warnings
}

func walQueueWarnings(wal *WALInfo) []Warning {
	if wal == nil || wal.QueueDepth == nil || wal.QueueCapacity == nil || *wal.QueueCapacity == 0 {
		return nil
	}

	ratio := float64(*wal.QueueDepth) / float64(*wal.QueueCapacity)
	if ratio < walQueuePressureThreshold {
		return nil
	}

	return []Warning{{
		Code:     "wal_queue_pressure",
		Severity: severityWarn,
		Message:  fmt.Sprintf("WAL queue is %.0f%% full", ratio*100),
		Details: map[string]any{
			"depth":    *wal.QueueDepth,
			"capacity": *wal.QueueCapacity,
		},
	}}
}

func replWarnings(repl *ReplInfo) []Warning {
	if repl == nil {
		return nil
	}

	var warnings []Warning
	for _, r := range repl.Replicas {
		switch {
		case r.Stale:
			warnings = append(warnings, Warning{
				Code:     "replica_stale",
				Severity: severityCrit,
				Message:  fmt.Sprintf("replica %q has not acked in %.0fs", r.ReplicaID, r.LagSec),
				Details: map[string]any{
					"replica_id": r.ReplicaID,
					"lag_sec":    r.LagSec,
				},
			})
		case r.LagSec >= replicaLagWarnThresholdSec:
			warnings = append(warnings, Warning{
				Code:     "replica_lag",
				Severity: severityWarn,
				Message:  fmt.Sprintf("replica %q lag is %.1fs", r.ReplicaID, r.LagSec),
				Details: map[string]any{
					"replica_id": r.ReplicaID,
					"lag_sec":    r.LagSec,
				},
			})
		}
	}

	return warnings
}

func streamWarnings(streams *StreamsInfo) []Warning {
	if streams == nil {
		return nil
	}

	var warnings []Warning
	for _, s := range append(append([]SubscriberInfo{}, streams.LimitSubscribers...), streams.QuotaSubscribers...) {
		if s.Dropped == 0 {
			continue
		}

		warnings = append(warnings, Warning{
			Code:     "stream_subscriber_dropping_events",
			Severity: severityWarn,
			Message:  "a stream subscriber is dropping events because its queue is full",
			Details: map[string]any{
				"dropped":   s.Dropped,
				"queue_cap": s.QueueCap,
			},
		})
	}

	return warnings
}

func dumpWarnings(dump *DumpInfo) []Warning {
	if dump == nil || !dump.Enabled {
		return nil
	}

	var warnings []Warning

	if dump.LastDumpError != nil {
		warnings = append(warnings, Warning{
			Code:     "dump_failed",
			Severity: severityCrit,
			Message:  "the last dump attempt failed",
			Details:  map[string]any{"error": *dump.LastDumpError},
		})
	}

	if dump.LastDumpAt != nil && dump.IntervalSec != nil {
		expected := time.Unix(*dump.LastDumpAt, 0).
			Add(time.Duration(*dump.IntervalSec*float64(time.Second)) * dumpOverdueFactor)
		if time.Now().After(expected) {
			warnings = append(warnings, Warning{
				Code:     "dump_overdue",
				Severity: severityWarn,
				Message:  "dump has not run within the expected interval",
				Details:  map[string]any{"last_dump_at": *dump.LastDumpAt},
			})
		}
	}

	return warnings
}

func durabilityWarnings(persistence *PersistenceInfo, wal *WALInfo) []Warning {
	if persistence == nil {
		return nil
	}

	if persistence.Mode == config.PersistenceModeMemory {
		return []Warning{{
			Code:     "no_durability",
			Severity: severityWarn,
			Message:  "persistence mode is memory: all data is lost on restart",
		}}
	}

	if wal != nil && wal.SyncCommit != nil && *wal.SyncCommit == config.WALSyncCommitOff {
		return []Warning{{
			Code:     "async_wal",
			Severity: severityWarn,
			Message:  "wal.sync_commit is off: acknowledged writes can be lost on crash",
		}}
	}

	return nil
}
