package inspect

import (
	"errors"
	"os"
	"sort"
	"time"

	"github.com/fq-db/fq/internal/database/storage"
	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/protocol"
	"github.com/fq-db/fq/internal/version"
)

const (
	maxSummaryReplicas      = 5
	minReplicaStaleInterval = 30 * time.Second
	replicaStaleFactor      = 5
)

func (i *Inspector) snapshot() observability.Snapshot {
	snap, _ := observability.GetSnapshot()

	return snap
}

func (i *Inspector) buildInstance(snap observability.Snapshot) *InstanceInfo {
	v := version.Get()

	role := "standalone"
	var replicaID *string
	switch {
	case i.deps.Master != nil:
		role = "master"
	case i.deps.Slave != nil:
		role = "slave"
		id := i.deps.Cfg.Replication.ReplicaID
		replicaID = &id
	}

	return &InstanceInfo{
		Version:         v.Version,
		Commit:          v.Commit,
		BuildDate:       v.Date,
		GoVersion:       v.GoVersion,
		Platform:        v.Platform,
		ProtocolVersion: int(protocol.CurrentVersion),
		UptimeSec:       time.Since(i.deps.StartedAt).Seconds(),
		PID:             os.Getpid(),
		Role:            role,
		ReplicaID:       replicaID,
		ListenAddr:      i.deps.Cfg.Network.Address,
		Connections:     int(snap.TCPActiveConnections),
	}
}

func (i *Inspector) buildPersistence() *PersistenceInfo {
	info := &PersistenceInfo{Mode: i.deps.Cfg.PersistenceMode()}

	if i.deps.Cfg.UsesWAL() && i.deps.Cfg.WAL != nil {
		syncCommit := i.deps.Cfg.WAL.SyncCommit
		info.SyncCommit = &syncCommit
	}

	return info
}

func (i *Inspector) buildWAL(snap observability.Snapshot) *WALInfo {
	info := &WALInfo{Enabled: i.deps.WAL != nil}

	if !i.deps.Cfg.UsesWAL() || i.deps.Cfg.WAL == nil {
		return info
	}

	syncCommit := i.deps.Cfg.WAL.SyncCommit
	info.SyncCommit = &syncCommit
	dataDirectory := i.deps.Cfg.WAL.DataDirectory
	info.DataDirectory = &dataDirectory

	if i.deps.Storage != nil {
		lsn := i.deps.Storage.CurrentLSN()
		info.LSNAssigned = lsn
	}

	if i.deps.WAL == nil {
		return info
	}

	depth := i.deps.WAL.QueueDepth()
	info.QueueDepth = &depth
	queueCapacity := i.deps.WAL.QueueCapacity()
	info.QueueCapacity = &queueCapacity

	if lsn, ok := i.deps.WAL.LastSyncedLSN(); ok {
		info.LSNFlushed = &lsn
	}

	if path, size, ok := i.deps.WAL.SegmentInfo(); ok && path != "" {
		info.SegmentPath = &path
		info.SegmentSizeBytes = &size
	}

	if at, duration, ok := i.deps.WAL.LastFlush(); ok {
		flushAt := at.Unix()
		info.LastFlushAt = &flushAt
		durationMs := float64(duration.Microseconds()) / 1000
		info.LastFlushDurationMs = &durationMs
	}

	if snap.WALFlushTotal > 0 {
		total := snap.WALFlushTotal
		info.FlushTotal = &total
		avgMs := snap.WALFlushAvgDurationSec * 1000
		info.FlushAvgDurationMs = &avgMs
	}

	return info
}

func (i *Inspector) buildDump() *DumpInfo {
	info := &DumpInfo{Enabled: i.deps.Cfg.UsesDump()}

	if !info.Enabled {
		return info
	}

	directory := i.deps.Cfg.Dump.Directory
	info.Directory = &directory
	intervalSec := i.deps.Cfg.Dump.Interval.Seconds()
	info.IntervalSec = &intervalSec

	if i.deps.Storage != nil {
		at, duration, tx, dumpErr := i.deps.Storage.LastDump()
		if !errors.Is(dumpErr, storage.ErrDumpNeverRun) {
			lastDumpAt := at.Unix()
			info.LastDumpAt = &lastDumpAt
			durationMs := float64(duration.Microseconds()) / 1000
			info.LastDumpDurationMs = &durationMs
			lastDumpTx := uint64(tx)
			info.LastDumpTx = &lastDumpTx
			if dumpErr != nil {
				errStr := dumpErr.Error()
				info.LastDumpError = &errStr
			}

			nextDumpAt := at.Add(i.deps.Cfg.Dump.Interval).Unix()
			info.NextDumpAt = &nextDumpAt
		}
	}

	if i.deps.Dumper != nil {
		path := i.deps.Dumper.CurrentDumpPath()
		info.CurrentDumpPath = &path
		if stat, err := os.Stat(path); err == nil {
			size := stat.Size()
			info.CurrentDumpSizeBytes = &size
		}
	}

	return info
}

func (i *Inspector) buildRepl(truncate bool) *ReplInfo {
	info := &ReplInfo{Role: "none", ProtocolVersion: int(replication.ProtocolVersion)}

	switch {
	case i.deps.Master != nil:
		info.Role = "master"
		info.Replicas = i.buildMasterReplicas(truncate, &info.Truncated)
		info.KnownReplicas = len(info.Replicas)
		if info.Truncated {
			info.KnownReplicas = i.masterReplicaCount()
		}
	case i.deps.Slave != nil:
		info.Role = "slave"
		info.Slave = buildSlaveInfo(i.deps.Slave.Status())
	}

	return info
}

func (i *Inspector) masterReplicaCount() int {
	return len(i.deps.Master.ReplicaCursors())
}

func (i *Inspector) buildMasterReplicas(truncate bool, truncated *bool) []ReplicaInfo {
	cursors := i.deps.Master.ReplicaCursors()

	var currentLSN uint64
	if i.deps.Storage != nil {
		currentLSN = i.deps.Storage.CurrentLSN()
	}

	staleAfter := replicaStaleThreshold(i.deps.Cfg.Replication.SyncInterval)

	replicas := make([]ReplicaInfo, 0, len(cursors))
	for _, c := range cursors {
		var lag uint64
		if currentLSN > c.LastAppliedLSN {
			lag = currentLSN - c.LastAppliedLSN
		}
		lagSec := time.Since(c.UpdatedAt).Seconds()

		replicas = append(replicas, ReplicaInfo{
			ReplicaID:       c.ReplicaID,
			LastSegmentName: c.LastSegmentName,
			LastAppliedLSN:  c.LastAppliedLSN,
			LagLSN:          lag,
			LagSec:          lagSec,
			Stale:           time.Since(c.UpdatedAt) > staleAfter,
			UpdatedAt:       c.UpdatedAt.Unix(),
		})
	}

	sort.Slice(replicas, func(a, b int) bool { return replicas[a].ReplicaID < replicas[b].ReplicaID })

	if truncate && len(replicas) > maxSummaryReplicas {
		*truncated = true
		return replicas[:maxSummaryReplicas]
	}

	return replicas
}

func replicaStaleThreshold(syncInterval time.Duration) time.Duration {
	if syncInterval <= 0 {
		return minReplicaStaleInterval
	}

	threshold := syncInterval * replicaStaleFactor
	if threshold < minReplicaStaleInterval {
		return minReplicaStaleInterval
	}

	return threshold
}

func buildSlaveInfo(status replication.SlaveStatus) *SlaveInfo {
	var lastReconnectAt *int64
	if !status.LastReconnectAt.IsZero() {
		unix := status.LastReconnectAt.Unix()
		lastReconnectAt = &unix
	}

	return &SlaveInfo{
		MasterAddress:     status.MasterAddress,
		Connected:         status.Connected,
		LastSegmentName:   status.LastSegmentName,
		LastAppliedLSN:    status.LastAppliedLSN,
		ConsecutiveErrors: status.ConsecutiveErrors,
		LastErrorCode:     int(status.LastErrorCode),
		ReconnectTotal:    status.ReconnectTotal,
		LastReconnectAt:   lastReconnectAt,
		UpdatedAt:         status.UpdatedAt.Unix(),
	}
}

func (i *Inspector) buildEngine(includePerPartition bool) *EngineInfo {
	info := &EngineInfo{KeyIndexEnabled: i.deps.Cfg.Engine.KeyIndex}

	if i.deps.Storage == nil {
		return info
	}

	stats := i.deps.Storage.EngineStats()
	info.Partitions = len(stats.Partitions)
	info.Counters = stats.Counters
	info.SlidingWindows = stats.SlidingWindows
	info.TokenBuckets = stats.TokenBuckets
	info.Quotas = stats.Quotas
	info.QuotaAllocations = stats.QuotaAllocations

	if includePerPartition {
		info.PerPartition = make([]PartitionStat, len(stats.Partitions))
		for idx, p := range stats.Partitions {
			info.PerPartition[idx] = PartitionStat{
				Index:            p.Index,
				Counters:         p.Counters,
				SlidingWindows:   p.SlidingWindows,
				TokenBuckets:     p.TokenBuckets,
				Quotas:           p.Quotas,
				QuotaAllocations: p.QuotaAllocations,
			}
		}
	}

	return info
}

func (i *Inspector) buildStreams() *StreamsInfo {
	info := &StreamsInfo{
		LimitSubscribers: []SubscriberInfo{},
		QuotaSubscribers: []SubscriberInfo{},
	}

	if i.deps.Storage == nil {
		return info
	}

	stats := i.deps.Storage.StreamStats()

	info.LimitSubscribers = make([]SubscriberInfo, len(stats.LimitSubscribers))
	for idx, s := range stats.LimitSubscribers {
		info.LimitSubscribers[idx] = toSubscriberInfo(s)
		info.LimitEventsDroppedTotal += s.Dropped
	}

	info.QuotaSubscribers = make([]SubscriberInfo, len(stats.QuotaSubscribers))
	for idx, s := range stats.QuotaSubscribers {
		info.QuotaSubscribers[idx] = toSubscriberInfo(s)
		info.QuotaEventsDroppedTotal += s.Dropped
	}

	return info
}

func toSubscriberInfo(s storage.SubscriberStat) SubscriberInfo {
	var prefix *string
	if s.HasPrefix {
		p := s.Prefix
		prefix = &p
	}

	return SubscriberInfo{
		Prefix:   prefix,
		QueueLen: s.QueueLen,
		QueueCap: s.QueueCap,
		Dropped:  s.Dropped,
	}
}
