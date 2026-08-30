package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Snapshot struct {
	TCPActiveConnections              float64
	WALQueueDepth                     float64
	WALFlushTotal                     float64
	WALFlushAvgDurationSec            float64
	ReplicationLagLSN                 float64
	ReplicationReconnectTotal         float64
	ReplicationReconnectAttemptsTotal float64
	ReplicationKnownReplicas          float64
}

func GetSnapshot() (Snapshot, error) {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return Snapshot{}, err
	}

	var snap Snapshot
	for _, mf := range families {
		switch mf.GetName() {
		case "fq_tcp_active_connections":
			snap.TCPActiveConnections = firstGaugeValue(mf)
		case "fq_wal_queue_depth":
			snap.WALQueueDepth = firstGaugeValue(mf)
		case "fq_wal_flush_total":
			snap.WALFlushTotal = firstCounterValue(mf)
		case "fq_wal_flush_duration_seconds":
			snap.WALFlushAvgDurationSec = firstHistogramAvg(mf)
		case "fq_replication_lag_lsn":
			snap.ReplicationLagLSN = firstGaugeValue(mf)
		case "fq_replication_reconnect_total":
			snap.ReplicationReconnectTotal = firstCounterValue(mf)
		case "fq_replication_reconnect_attempts_total":
			snap.ReplicationReconnectAttemptsTotal = firstCounterValue(mf)
		case "fq_replication_known_replicas":
			snap.ReplicationKnownReplicas = firstGaugeValue(mf)
		}
	}

	return snap, nil
}

func firstGaugeValue(mf *dto.MetricFamily) float64 {
	if len(mf.GetMetric()) == 0 {
		return 0
	}

	return mf.GetMetric()[0].GetGauge().GetValue()
}

func firstCounterValue(mf *dto.MetricFamily) float64 {
	if len(mf.GetMetric()) == 0 {
		return 0
	}

	return mf.GetMetric()[0].GetCounter().GetValue()
}

func firstHistogramAvg(mf *dto.MetricFamily) float64 {
	if len(mf.GetMetric()) == 0 {
		return 0
	}

	histogram := mf.GetMetric()[0].GetHistogram()
	count := histogram.GetSampleCount()
	if count == 0 {
		return 0
	}

	return histogram.GetSampleSum() / float64(count)
}
