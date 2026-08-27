package stress

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEventLogKeepsMostRecentEventsInOrder(t *testing.T) {
	log := NewEventLog(3)
	log.Add(Event{Kind: "one"})
	log.Add(Event{Kind: "two"})
	log.Add(Event{Kind: "three"})
	log.Add(Event{Kind: "four"})

	events := log.Snapshot()
	if len(events) != 3 {
		t.Fatalf("events len = %d", len(events))
	}
	for i, want := range []string{"two", "three", "four"} {
		if events[i].Kind != want {
			t.Fatalf("event %d = %q, want %q", i, events[i].Kind, want)
		}
	}
}

func TestFinishScenarioWritesFailureReportAndKeepsData(t *testing.T) {
	workDir := t.TempDir()
	reportPath := filepath.Join(workDir, "report", "stress-result.json")
	env, err := NewEnvironment(Options{
		Scenario:   CrashLoopScenario,
		Seed:       7,
		WorkDir:    filepath.Join(workDir, "data"),
		ReportFile: reportPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	result := Result{Scenario: CrashLoopScenario, Operations: 3}
	runErr := errors.New("boom")
	events := NewEventLog(10)
	events.Add(Event{Kind: "write_ok", Key: "stress_counter_001", Value: 3})

	runErr = finishScenario(
		Options{Scenario: CrashLoopScenario, Seed: 7, ReportFile: reportPath},
		env,
		time.Now(),
		&result,
		runErr,
		events,
		func() map[string]uint64 {
			return map[string]uint64{"stress_counter_001": 3}
		},
	)

	if runErr == nil || !strings.Contains(runErr.Error(), "stress report: "+reportPath) {
		t.Fatalf("run error does not include report path: %v", runErr)
	}
	if _, err := os.Stat(env.RootDir); err != nil {
		t.Fatalf("stress data was not kept: %v", err)
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{
		`"status": "failed"`,
		`"failure": "boom"`,
		`"kind": "write_ok"`,
		`"stress_counter_001": 3`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("report does not contain %q:\n%s", want, text)
		}
	}
}
