package generator

import (
	"testing"
	"time"
)

func TestLoadConfigUsesCLIValues(t *testing.T) {
	cfg, err := LoadConfig([]string{
		"--target-url", "http://ingestion:8080/api/v1/telemetry",
		"--mode", "spike",
		"--devices", "20",
		"--patients", "10",
		"--duration", "2m",
		"--batch-size", "2",
		"--max-batch-size", "8",
		"--base-interval", "3s",
		"--rps-limit", "250",
		"--output", "results/test.json",
	})
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Mode != ModeSpike || cfg.Devices != 20 || cfg.Patients != 10 {
		t.Fatalf("config = %+v", cfg)
	}
	if cfg.Duration != 2*time.Minute || cfg.BatchSize != 2 || cfg.MaxBatchSize != 8 {
		t.Fatalf("config durations/batches = %+v", cfg)
	}
	if cfg.BaseInterval != 3*time.Second || cfg.RPSLimit != 250 {
		t.Fatalf("config pacing = %+v", cfg)
	}
}

func TestConfigRejectsContractViolations(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "unsupported mode", args: []string{"--mode", "unknown"}},
		{name: "zero devices", args: []string{"--devices", "0"}},
		{name: "batch too large", args: []string{"--batch-size", "11"}},
		{name: "max below normal batch", args: []string{"--batch-size", "5", "--max-batch-size", "4"}},
		{name: "relative target", args: []string{"--target-url", "/api/v1/telemetry"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := LoadConfig(test.args); err == nil {
				t.Fatal("LoadConfig() error = nil")
			}
		})
	}
}
