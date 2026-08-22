package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

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
	)
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
