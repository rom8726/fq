package dbcli

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/logrusorgru/aurora/v4"

	"github.com/fq-db/fq/internal/inspect"
)

const dash = "—"

func renderReport(w io.Writer, report *inspect.Report) {
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)

	_, _ = fmt.Fprintf(w, "%s\n", aurora.Bold(aurora.Cyan(fmt.Sprintf("fq inspect: %s", report.Section))))
	_, _ = fmt.Fprintf(w, "%s\n", aurora.Faint(time.Unix(report.TS, 0).Local().Format("2006-01-02 15:04:05 MST")))

	if report.Instance != nil {
		renderInstance(w, tw, report.Instance)
	}
	if report.Persistence != nil {
		renderPersistence(w, tw, report.Persistence)
	}
	if report.WAL != nil {
		renderWAL(w, tw, report.WAL)
	}
	if report.Dump != nil {
		renderDump(w, tw, report.Dump)
	}
	if report.Repl != nil {
		renderRepl(w, tw, report.Repl)
	}
	if report.Engine != nil {
		renderEngine(w, tw, report.Engine)
	}
	if report.Streams != nil {
		renderStreams(w, tw, report.Streams)
	}
	if len(report.Warnings) > 0 {
		renderWarnings(w, report.Warnings)
	}
}

func sectionTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, aurora.Bold(title))
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)))
}

func renderInstance(w io.Writer, tw *tabwriter.Writer, info *inspect.InstanceInfo) {
	sectionTitle(w, "INSTANCE")

	_, _ = fmt.Fprintf(tw, "role\t%s\n", roleColored(info.Role))
	if info.ReplicaID != nil {
		_, _ = fmt.Fprintf(tw, "replica_id\t%s\n", *info.ReplicaID)
	}
	_, _ = fmt.Fprintf(tw, "version\t%s (%s)\n", info.Version, shortCommit(info.Commit))
	_, _ = fmt.Fprintf(tw, "build_date\t%s\n", orDash(info.BuildDate))
	_, _ = fmt.Fprintf(tw, "runtime\t%s %s\n", info.GoVersion, info.Platform)
	_, _ = fmt.Fprintf(tw, "uptime\t%s\n", formatUptime(info.UptimeSec))
	_, _ = fmt.Fprintf(tw, "pid\t%d\n", info.PID)
	_, _ = fmt.Fprintf(tw, "listen\t%s\n", info.ListenAddr)
	_, _ = fmt.Fprintf(tw, "connections\t%d\n", info.Connections)

	_ = tw.Flush()
}

func renderPersistence(w io.Writer, tw *tabwriter.Writer, info *inspect.PersistenceInfo) {
	sectionTitle(w, "PERSISTENCE")

	_, _ = fmt.Fprintf(tw, "mode\t%s\n", durabilityColored(info.Mode))
	_, _ = fmt.Fprintf(tw, "sync_commit\t%s\n", strVal(info.SyncCommit))

	_ = tw.Flush()
}

func renderWAL(w io.Writer, tw *tabwriter.Writer, info *inspect.WALInfo) {
	sectionTitle(w, "WAL")

	_, _ = fmt.Fprintf(tw, "enabled\t%s\n", yesNo(info.Enabled))
	if !info.Enabled {
		_ = tw.Flush()
		return
	}

	_, _ = fmt.Fprintf(tw, "sync_commit\t%s\n", strVal(info.SyncCommit))
	_, _ = fmt.Fprintf(tw, "data_directory\t%s\n", strVal(info.DataDirectory))
	_, _ = fmt.Fprintf(tw, "lsn assigned / flushed\t%d / %s\n", info.LSNAssigned, uint64Val(info.LSNFlushed))
	_, _ = fmt.Fprintf(tw, "queue\t%s\n", queueLoad(info.QueueDepth, info.QueueCapacity))
	_, _ = fmt.Fprintf(tw, "segment\t%s (%s)\n", strVal(info.SegmentPath), byteSizeVal(info.SegmentSizeBytes))
	_, _ = fmt.Fprintf(tw, "last flush\t%s, took %s\n", unixVal(info.LastFlushAt), msVal(info.LastFlushDurationMs))
	_, _ = fmt.Fprintf(tw, "flush avg / total\t%s / %s\n", msVal(info.FlushAvgDurationMs), countVal(info.FlushTotal))

	_ = tw.Flush()
}

func renderDump(w io.Writer, tw *tabwriter.Writer, info *inspect.DumpInfo) {
	sectionTitle(w, "DUMP")

	_, _ = fmt.Fprintf(tw, "enabled\t%s\n", yesNo(info.Enabled))
	if !info.Enabled {
		_ = tw.Flush()
		return
	}

	_, _ = fmt.Fprintf(tw, "directory\t%s\n", strVal(info.Directory))
	_, _ = fmt.Fprintf(tw, "interval\t%s\n", secVal(info.IntervalSec))
	_, _ = fmt.Fprintf(
		tw, "current dump\t%s (%s)\n", strVal(info.CurrentDumpPath), byteSizeVal64(info.CurrentDumpSizeBytes),
	)

	if info.LastDumpError != nil {
		_, _ = fmt.Fprintf(tw, "last dump\t%s\n", aurora.Red("failed: "+*info.LastDumpError))
	} else {
		_, _ = fmt.Fprintf(tw, "last dump\t%s, took %s\n", unixVal(info.LastDumpAt), msVal(info.LastDumpDurationMs))
	}
	_, _ = fmt.Fprintf(tw, "next dump\t%s\n", relativeUnixVal(info.NextDumpAt))

	_ = tw.Flush()
}

func renderRepl(w io.Writer, tw *tabwriter.Writer, info *inspect.ReplInfo) {
	sectionTitle(w, "REPLICATION")

	_, _ = fmt.Fprintf(tw, "role\t%s\n", roleColored(info.Role))
	_ = tw.Flush()

	switch info.Role {
	case "master":
		renderReplicasTable(w, info)
	case "slave":
		renderSlaveInfo(tw, info.Slave)
		_ = tw.Flush()
	}
}

func renderReplicasTable(w io.Writer, info *inspect.ReplInfo) {
	if len(info.Replicas) == 0 {
		_, _ = fmt.Fprintln(w, aurora.Faint("no known replicas"))
		return
	}

	table := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "REPLICA ID\tLAST SEGMENT\tLAST APPLIED LSN\tLAG (LSN)\tLAG (SEC)\tSTATUS")
	for _, r := range info.Replicas {
		_, _ = fmt.Fprintf(
			table, "%s\t%s\t%d\t%d\t%.1fs\t%s\n",
			r.ReplicaID, orDash(r.LastSegmentName), r.LastAppliedLSN, r.LagLSN, r.LagSec, replicaStatus(r.Stale),
		)
	}
	_ = table.Flush()

	if info.Truncated {
		note := fmt.Sprintf(
			"showing %d of %d replicas — use INSPECT ALL for the full list", len(info.Replicas), info.KnownReplicas,
		)
		_, _ = fmt.Fprintf(w, "%s\n", aurora.Faint(note))
	}
}

func renderSlaveInfo(tw *tabwriter.Writer, slave *inspect.SlaveInfo) {
	if slave == nil {
		return
	}

	_, _ = fmt.Fprintf(tw, "master\t%s\n", slave.MasterAddress)
	_, _ = fmt.Fprintf(tw, "connected\t%s\n", connectedColored(slave.Connected))
	_, _ = fmt.Fprintf(tw, "last segment\t%s\n", orDash(slave.LastSegmentName))
	_, _ = fmt.Fprintf(tw, "last applied lsn\t%d\n", slave.LastAppliedLSN)
	_, _ = fmt.Fprintf(tw, "consecutive errors\t%s\n", errorCountColored(slave.ConsecutiveErrors))
	_, _ = fmt.Fprintf(tw, "last error code\t%d\n", slave.LastErrorCode)
	_, _ = fmt.Fprintf(tw, "reconnects\t%d (last %s)\n", slave.ReconnectTotal, unixVal(slave.LastReconnectAt))
}

func renderEngine(w io.Writer, tw *tabwriter.Writer, info *inspect.EngineInfo) {
	sectionTitle(w, "ENGINE")

	_, _ = fmt.Fprintf(tw, "partitions\t%d\n", info.Partitions)
	_, _ = fmt.Fprintf(tw, "key index\t%s\n", yesNo(info.KeyIndexEnabled))
	_, _ = fmt.Fprintf(tw, "counters\t%d\n", info.Counters)
	_, _ = fmt.Fprintf(tw, "sliding windows\t%d\n", info.SlidingWindows)
	_, _ = fmt.Fprintf(tw, "token buckets\t%d\n", info.TokenBuckets)
	_, _ = fmt.Fprintf(tw, "quotas\t%d\n", info.Quotas)
	_, _ = fmt.Fprintf(tw, "quota allocations\t%d\n", info.QuotaAllocations)
	_ = tw.Flush()

	if len(info.PerPartition) == 0 {
		return
	}

	_, _ = fmt.Fprintln(w)
	table := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "PARTITION\tCOUNTERS\tSLIDING WINDOWS\tTOKEN BUCKETS\tQUOTAS\tALLOCATIONS")
	for _, p := range info.PerPartition {
		_, _ = fmt.Fprintf(
			table, "%d\t%d\t%d\t%d\t%d\t%d\n",
			p.Index, p.Counters, p.SlidingWindows, p.TokenBuckets, p.Quotas, p.QuotaAllocations,
		)
	}
	_ = table.Flush()
}

func renderStreams(w io.Writer, tw *tabwriter.Writer, info *inspect.StreamsInfo) {
	sectionTitle(w, "STREAMS")

	_, _ = fmt.Fprintf(
		tw, "limit subscribers\t%d (dropped %s)\n", len(info.LimitSubscribers), droppedColored(info.LimitEventsDroppedTotal),
	)
	_, _ = fmt.Fprintf(
		tw, "quota subscribers\t%d (dropped %s)\n", len(info.QuotaSubscribers), droppedColored(info.QuotaEventsDroppedTotal),
	)
	_ = tw.Flush()

	renderSubscriberTable(w, "LIMIT SUBSCRIBERS", info.LimitSubscribers)
	renderSubscriberTable(w, "QUOTA SUBSCRIBERS", info.QuotaSubscribers)
}

func renderSubscriberTable(w io.Writer, title string, subs []inspect.SubscriberInfo) {
	if len(subs) == 0 {
		return
	}

	_, _ = fmt.Fprintf(w, "\n%s\n", aurora.Faint(title))
	table := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "PREFIX\tQUEUE\tDROPPED")
	for _, s := range subs {
		prefix := "*"
		if s.Prefix != nil {
			prefix = *s.Prefix
		}
		_, _ = fmt.Fprintf(table, "%s\t%d/%d\t%s\n", prefix, s.QueueLen, s.QueueCap, droppedColored(s.Dropped))
	}
	_ = table.Flush()
}

func renderWarnings(w io.Writer, warnings []inspect.Warning) {
	sectionTitle(w, "WARNINGS")

	for _, warning := range warnings {
		icon := aurora.Yellow("[warn]")
		if warning.Severity == "crit" {
			icon = aurora.Red("[crit]")
		}

		_, _ = fmt.Fprintf(w, "%s %s\n", icon, warning.Message)
		if len(warning.Details) > 0 {
			_, _ = fmt.Fprintf(w, "       %s\n", aurora.Faint(formatDetails(warning.Details)))
		}
	}
}

func formatDetails(details map[string]any) string {
	parts := make([]string, 0, len(details))
	for k, v := range details {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	sort.Strings(parts)

	return strings.Join(parts, "  ")
}

func roleColored(role string) string {
	switch role {
	case "master":
		return aurora.Cyan(role).Bold().String()
	case "slave":
		return aurora.Magenta(role).Bold().String()
	default:
		return role
	}
}

func durabilityColored(mode string) string {
	if mode == "memory" {
		return aurora.Yellow(mode).String()
	}

	return mode
}

func connectedColored(connected bool) string {
	if connected {
		return aurora.Green("yes").String()
	}

	return aurora.Red("no").String()
}

func replicaStatus(stale bool) string {
	if stale {
		return aurora.Red("stale").String()
	}

	return aurora.Green("ok").String()
}

func errorCountColored(n int) string {
	if n == 0 {
		return "0"
	}

	return aurora.Red(strconv.Itoa(n)).String()
}

func droppedColored(n uint64) string {
	if n == 0 {
		return "0"
	}

	return aurora.Red(strconv.FormatUint(n, 10)).String()
}

func queueLoad(depth, capacity *int) string {
	if depth == nil || capacity == nil || *capacity == 0 {
		return dash
	}

	ratio := float64(*depth) / float64(*capacity)
	text := fmt.Sprintf("%d / %d (%.0f%%)", *depth, *capacity, ratio*100)

	switch {
	case ratio >= 0.9:
		return aurora.Red(text).String()
	case ratio >= 0.75:
		return aurora.Yellow(text).String()
	default:
		return text
	}
}

func yesNo(b bool) string {
	if b {
		return aurora.Green("yes").String()
	}

	return aurora.Faint("no").String()
}

func orDash(s string) string {
	if s == "" {
		return dash
	}

	return s
}

func strVal(p *string) string {
	if p == nil {
		return dash
	}

	return *p
}

func uint64Val(p *uint64) string {
	if p == nil {
		return dash
	}

	return strconv.FormatUint(*p, 10)
}

func msVal(p *float64) string {
	if p == nil {
		return dash
	}

	return fmt.Sprintf("%.2fms", *p)
}

func secVal(p *float64) string {
	if p == nil {
		return dash
	}

	return time.Duration(*p * float64(time.Second)).String()
}

func countVal(p *float64) string {
	if p == nil {
		return dash
	}

	return strconv.FormatInt(int64(*p), 10)
}

func byteSizeVal(p *int) string {
	if p == nil {
		return dash
	}

	return formatBytes(int64(*p))
}

func byteSizeVal64(p *int64) string {
	if p == nil {
		return dash
	}

	return formatBytes(*p)
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}

	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func unixVal(p *int64) string {
	if p == nil {
		return dash
	}

	return time.Unix(*p, 0).Local().Format("2006-01-02 15:04:05")
}

func relativeUnixVal(p *int64) string {
	if p == nil {
		return dash
	}

	delta := time.Until(time.Unix(*p, 0))
	if delta < 0 {
		return aurora.Red(fmt.Sprintf("overdue by %s", (-delta).Round(time.Second))).String()
	}

	return fmt.Sprintf("in %s", delta.Round(time.Second))
}

func formatUptime(seconds float64) string {
	return time.Duration(seconds * float64(time.Second)).Round(time.Second).String()
}

func shortCommit(commit string) string {
	const shortLen = 12
	if len(commit) <= shortLen {
		return commit
	}

	return commit[:shortLen]
}
