package stress

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const defaultEventLimit = 200

type Report struct {
	Scenario         string            `json:"scenario"`
	Status           string            `json:"status"`
	StartedAt        time.Time         `json:"started_at"`
	FinishedAt       time.Time         `json:"finished_at"`
	DurationMillis   int64             `json:"duration_millis"`
	Options          Options           `json:"options"`
	Result           Result            `json:"result"`
	Failure          string            `json:"failure,omitempty"`
	Environment      ReportEnvironment `json:"environment"`
	ExpectedCounters map[string]uint64 `json:"expected_counters,omitempty"`
	LastEvents       []Event           `json:"last_events,omitempty"`
}

type ReportEnvironment struct {
	RootDir    string `json:"root_dir"`
	ConfigPath string `json:"config_path"`
	WALDir     string `json:"wal_dir"`
	DumpDir    string `json:"dump_dir"`
	StdoutPath string `json:"stdout_path"`
	StderrPath string `json:"stderr_path"`
	ReportPath string `json:"report_path"`
	Address    string `json:"address"`
}

type Event struct {
	At       time.Time `json:"at"`
	Kind     string    `json:"kind"`
	Worker   int       `json:"worker,omitempty"`
	Key      string    `json:"key,omitempty"`
	Value    uint64    `json:"value,omitempty"`
	Query    string    `json:"query,omitempty"`
	Response string    `json:"response,omitempty"`
	Error    string    `json:"error,omitempty"`
}

type EventLog struct {
	mu     sync.Mutex
	events []Event
	next   int
	full   bool
	limit  int
}

func NewEventLog(limit int) *EventLog {
	if limit <= 0 {
		limit = defaultEventLimit
	}

	return &EventLog{
		events: make([]Event, 0, limit),
		limit:  limit,
	}
}

func (l *EventLog) Add(event Event) {
	if l == nil {
		return
	}
	if event.At.IsZero() {
		event.At = time.Now()
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.events) < l.limit {
		l.events = append(l.events, event)

		return
	}

	l.events[l.next] = event
	l.next = (l.next + 1) % l.limit
	l.full = true
}

func (l *EventLog) Snapshot() []Event {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.full {
		out := make([]Event, len(l.events))
		copy(out, l.events)

		return out
	}

	out := make([]Event, 0, l.limit)
	out = append(out, l.events[l.next:]...)
	out = append(out, l.events[:l.next]...)

	return out
}

func WriteReport(report Report) error {
	if report.Environment.ReportPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(report.Environment.ReportPath), 0o750); err != nil {
		return fmt.Errorf("create stress report dir: %w", err)
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal stress report: %w", err)
	}
	data = append(data, '\n')

	if err := os.WriteFile(report.Environment.ReportPath, data, 0o644); err != nil {
		return fmt.Errorf("write stress report: %w", err)
	}

	return nil
}

func ReportEnvironmentFrom(env *Environment) ReportEnvironment {
	return ReportEnvironment{
		RootDir:    env.RootDir,
		ConfigPath: env.ConfigPath,
		WALDir:     env.WALDir,
		DumpDir:    env.DumpDir,
		StdoutPath: env.StdoutPath,
		StderrPath: env.StderrPath,
		ReportPath: env.ReportPath,
		Address:    env.Address,
	}
}
