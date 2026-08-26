package stress

import (
	"context"
	"os"
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
