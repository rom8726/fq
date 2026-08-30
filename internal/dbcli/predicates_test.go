package dbcli_test

import (
	"testing"

	"github.com/fq-db/fq/internal/dbcli"
)

func TestIsWatchCommand(t *testing.T) {
	cases := map[string]bool{
		"WATCH foo": true,
		"watch foo": true,
		"WATCH":     false,
		"GET foo":   false,
		"":          false,
	}
	for input, want := range cases {
		if got := dbcli.IsWatchCommand(input); got != want {
			t.Errorf("IsWatchCommand(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestIsStreamCommand(t *testing.T) {
	cases := map[string]bool{
		"STREAM":       true,
		"stream":       true,
		"PSTREAM foo":  true,
		"QSTREAM":      true,
		"QPSTREAM foo": true,
		"STREAM foo":   false,
		"GET foo":      false,
	}
	for input, want := range cases {
		if got := dbcli.IsStreamCommand(input); got != want {
			t.Errorf("IsStreamCommand(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestIsInspectCommand(t *testing.T) {
	cases := map[string]bool{
		"INSPECT":     true,
		"inspect":     true,
		"INSPECT wal": true,
		"HINSPECT":    false,
		"GET foo":     false,
	}
	for input, want := range cases {
		if got := dbcli.IsInspectCommand(input); got != want {
			t.Errorf("IsInspectCommand(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestIsHumanInspectCommand(t *testing.T) {
	cases := map[string]bool{
		"HINSPECT":     true,
		"hinspect wal": true,
		"INSPECT":      false,
		"GET foo":      false,
	}
	for input, want := range cases {
		if got := dbcli.IsHumanInspectCommand(input); got != want {
			t.Errorf("IsHumanInspectCommand(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestIsQuitCommand(t *testing.T) {
	cases := map[string]bool{
		"q":       true,
		"quit":    true,
		"exit":    true,
		"Q":       false,
		"GET foo": false,
		"":        false,
	}
	for input, want := range cases {
		if got := dbcli.IsQuitCommand(input); got != want {
			t.Errorf("IsQuitCommand(%q) = %v, want %v", input, got, want)
		}
	}
}
