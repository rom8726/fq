package stress

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRestartSmokeScenarioIntegration(t *testing.T) {
	if os.Getenv("FQ_STRESS_INTEGRATION") != "1" {
		t.Skip("set FQ_STRESS_INTEGRATION=1 to run subprocess stress scenario")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	result, err := RunRestartSmoke(ctx, Options{
		Duration:      30 * time.Second,
		Seed:          42,
		RepositoryDir: "../..",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Operations != 10 {
		t.Fatalf("operations = %d", result.Operations)
	}
}

func TestCrashLoopScenarioIntegration(t *testing.T) {
	if os.Getenv("FQ_STRESS_INTEGRATION") != "1" {
		t.Skip("set FQ_STRESS_INTEGRATION=1 to run subprocess stress scenario")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	reportPath := filepath.Join(t.TempDir(), "crash-loop-report.json")

	result, err := RunCrashLoop(ctx, Options{
		Duration:       2 * time.Second,
		Seed:           42,
		Workers:        2,
		Keys:           5,
		KillInterval:   300 * time.Millisecond,
		RequestTimeout: 200 * time.Millisecond,
		RepositoryDir:  "../..",
		ReportFile:     reportPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Operations == 0 {
		t.Fatal("no operations were acknowledged")
	}
	if result.Restarts < 2 {
		t.Fatalf("restarts = %d", result.Restarts)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "passed" || report.Scenario != CrashLoopScenario {
		t.Fatalf("unexpected report status: %+v", report)
	}
	if len(report.LastEvents) == 0 {
		t.Fatal("report has no events")
	}
}
