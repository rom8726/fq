package main

import "testing"

func TestParseArgsUsesLatinCConfigFlag(t *testing.T) {
	opts, err := parseArgs([]string{"-c", "./config.yml"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}

	if opts.configPath != "./config.yml" {
		t.Fatalf("configPath = %q, want ./config.yml", opts.configPath)
	}
}

func TestParseArgsInteractiveConfigFlag(t *testing.T) {
	opts, err := parseArgs([]string{"-i", "-c", "./config.yml"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}

	if !opts.interactive {
		t.Fatal("interactive = false, want true")
	}
	if opts.configPath != "./config.yml" {
		t.Fatalf("configPath = %q, want ./config.yml", opts.configPath)
	}
}

func TestParseArgsPositionalConfigPath(t *testing.T) {
	opts, err := parseArgs([]string{"./config.yml"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}

	if opts.configPath != "./config.yml" {
		t.Fatalf("configPath = %q, want ./config.yml", opts.configPath)
	}
}
