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

	DumpCompressionInputBytes           float64
	DumpCompressionOutputBytes          float64
	ReplicationCompressionRejectedTotal float64
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
		case "fq_compression_input_bytes_total":
			snap.DumpCompressionInputBytes = labeledCounterValue(mf, "target", "dump")
		case "fq_compression_output_bytes_total":
			snap.DumpCompressionOutputBytes = labeledCounterValue(mf, "target", "dump")
		case "fq_replication_compression_rejected_total":
			snap.ReplicationCompressionRejectedTotal = firstCounterValue(mf)
		}
	}

	return snap, nil
}

func labeledCounterValue(mf *dto.MetricFamily, label, value string) float64 {
	for _, metric := range mf.GetMetric() {
		for _, pair := range metric.GetLabel() {
			if pair.GetName() == label && pair.GetValue() == value {
				return metric.GetCounter().GetValue()
			}
		}
	}

	return 0
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
