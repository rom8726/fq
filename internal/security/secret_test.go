package security_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

func TestSecretIsRedactedWhenFormatted(t *testing.T) {
	secret := security.Secret("super-secret-token-value")

	require.Equal(t, "[REDACTED]", secret.String())
	require.NotContains(t, fmtSprintf(secret), "super-secret")
	require.Equal(t, "super-secret-token-value", secret.Reveal())
	require.False(t, secret.Empty())
	require.True(t, security.Secret("").Empty())
}

func TestLoadSecretFromEnv(t *testing.T) {
	t.Setenv("FQ_TEST_TOKEN", "  env-token-value-1234  ")

	secret, err := security.LoadSecret("FQ_TEST_TOKEN", "")
	require.NoError(t, err)
	require.Equal(t, "env-token-value-1234", secret.Reveal())
}

func TestLoadSecretFromFileTrimsNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("file-token-value-1234\n"), 0o600))

	secret, err := security.LoadSecret("", path)
	require.NoError(t, err)
	require.Equal(t, "file-token-value-1234", secret.Reveal())
}

func TestLoadSecretRejectsAmbiguousSource(t *testing.T) {
	_, err := security.LoadSecret("", "")
	require.ErrorIs(t, err, security.ErrSecretSourceAmbiguous)

	_, err = security.LoadSecret("FQ_TEST_TOKEN", "/tmp/token")
	require.ErrorIs(t, err, security.ErrSecretSourceAmbiguous)
}

func TestLoadSecretRejectsEmptyAndShort(t *testing.T) {
	t.Setenv("FQ_TEST_EMPTY", "   ")
	_, err := security.LoadSecret("FQ_TEST_EMPTY", "")
	require.ErrorIs(t, err, security.ErrSecretEmpty)

	t.Setenv("FQ_TEST_SHORT", "short")
	_, err = security.LoadSecret("FQ_TEST_SHORT", "")
	require.ErrorIs(t, err, security.ErrSecretTooShort)
}

func TestLoadSecretReportsMissingFile(t *testing.T) {
	_, err := security.LoadSecret("", filepath.Join(t.TempDir(), "absent"))
	require.Error(t, err)
}

func fmtSprintf(value any) string {
	return fmt.Sprintf("%v|%s", value, value)
}
