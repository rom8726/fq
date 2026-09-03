package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const compressionTargetLabel = "target"

var (
	tcpActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fq_tcp_active_connections",
		Help: "Current number of active TCP client connections.",
	})

	walQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fq_wal_queue_depth",
		Help: "Current number of pending WAL records in the queue.",
	})
	walFlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fq_wal_flush_duration_seconds",
		Help:    "WAL flush duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})
	walFlushBatchRecords = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fq_wal_flush_batch_records",
		Help:    "Number of WAL records written in one flush.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192},
	})
	walFlushTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fq_wal_flush_total",
		Help: "Total number of WAL flushes.",
	})

	replicationLagLSN = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fq_replication_lag_lsn",
		Help: "Current slave replication lag in LSN units.",
	})
	replicationReconnectTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fq_replication_reconnect_total",
		Help: "Total number of successful replication reconnects.",
	})
	replicationReconnectAttemptsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fq_replication_reconnect_attempts_total",
		Help: "Total number of replication reconnect attempts.",
	})
	replicationReplicaLastAppliedLSN = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fq_replication_replica_last_applied_lsn",
		Help: "Last LSN acknowledged as applied by a replica.",
	}, []string{"replica_id"})
	replicationReplicaLastAckTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fq_replication_replica_last_ack_timestamp",
		Help: "Unix timestamp of the last WAL ack received from a replica.",
	}, []string{"replica_id"})
	replicationKnownReplicas = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fq_replication_known_replicas",
		Help: "Number of replicas known by the master replication tracker.",
	})

	authFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fq_auth_failures_total",
		Help: "Total number of rejected authentication attempts.",
	}, []string{"port"})

	protocolErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fq_protocol_errors_total",
		Help: "Total number of error responses by protocol error code.",
	}, []string{"code"})

	compressionInputBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fq_compression_input_bytes_total",
		Help: "Total number of bytes submitted to compression.",
	}, []string{compressionTargetLabel})
	compressionOutputBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fq_compression_output_bytes_total",
		Help: "Total number of bytes produced by compression.",
	}, []string{compressionTargetLabel})
	compressionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fq_compression_duration_seconds",
		Help:    "Compression and decompression duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{compressionTargetLabel, "op"})
	replicationCompressionRejectedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fq_replication_compression_rejected_total",
		Help: "Total number of WAL chunks not served because the replica lacks the compression codec.",
	})
)

func init() {
	prometheus.MustRegister(
		tcpActiveConnections,
		walQueueDepth,
		walFlushDuration,
		walFlushBatchRecords,
		walFlushTotal,
		replicationLagLSN,
		replicationReconnectTotal,
		replicationReconnectAttemptsTotal,
		replicationReplicaLastAppliedLSN,
		replicationReplicaLastAckTimestamp,
		replicationKnownReplicas,
		authFailuresTotal,
		protocolErrorsTotal,
		compressionInputBytes,
		compressionOutputBytes,
		compressionDuration,
		replicationCompressionRejectedTotal,
	)
}

func ObserveCompression(target string, input, output int) {
	compressionInputBytes.WithLabelValues(target).Add(float64(input))
	compressionOutputBytes.WithLabelValues(target).Add(float64(output))
}

func ObserveCompressionDuration(target, op string, duration time.Duration) {
	compressionDuration.WithLabelValues(target, op).Observe(duration.Seconds())
}

func IncReplicationCompressionRejected() {
	replicationCompressionRejectedTotal.Inc()
}

func IncAuthFailures(port string) {
	authFailuresTotal.WithLabelValues(port).Inc()
}

func IncProtocolError(code uint16) {
	protocolErrorsTotal.WithLabelValues(strconv.FormatUint(uint64(code), 10)).Inc()
}

func IncTCPActiveConnections() {
	tcpActiveConnections.Inc()
}

func DecTCPActiveConnections() {
	tcpActiveConnections.Dec()
}

func SetWALQueueDepth(depth int) {
	walQueueDepth.Set(float64(depth))
}

func ObserveWALFlushLatency(latency time.Duration) {
	walFlushDuration.Observe(latency.Seconds())
	walFlushTotal.Inc()
}

func ObserveWALFlushBatchSize(size int) {
	walFlushBatchRecords.Observe(float64(size))
}

func SetReplicationLagLSN(lag uint64) {
	replicationLagLSN.Set(float64(lag))
}

func IncReplicationReconnectTotal() {
	replicationReconnectTotal.Inc()
}

func IncReplicationReconnectAttemptsTotal() {
	replicationReconnectAttemptsTotal.Inc()
}

func SetReplicationReplicaLastAppliedLSN(replicaID string, lsn uint64) {
	replicationReplicaLastAppliedLSN.WithLabelValues(replicaID).Set(float64(lsn))
}

func SetReplicationReplicaLastAckTimestamp(replicaID string, timestamp time.Time) {
	replicationReplicaLastAckTimestamp.WithLabelValues(replicaID).Set(float64(timestamp.Unix()))
}

func SetReplicationKnownReplicas(count int) {
	replicationKnownReplicas.Set(float64(count))
}
