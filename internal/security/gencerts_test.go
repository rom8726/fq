package security_test

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

func runGenCerts(t *testing.T, dir string, extraEnv ...string) error {
	t.Helper()

	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl is not available")
	}

	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "gen-certs.sh"))
	require.NoError(t, err)

	cmd := exec.Command(script)
	cmd.Env = append(os.Environ(), "CERT_DIR="+dir)
	cmd.Env = append(cmd.Env, extraEnv...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("gen-certs.sh output:\n%s", output)
	}

	return err
}

func verifyServerCert(t *testing.T, dir, host string) error {
	t.Helper()

	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))

	serverPEM, err := os.ReadFile(filepath.Join(dir, "server.crt"))
	require.NoError(t, err)

	block, _ := pem.Decode(serverPEM)
	require.NotNil(t, block)

	certificate, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	_, err = certificate.Verify(x509.VerifyOptions{
		DNSName:   host,
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})

	return err
}

func requireVerifiesFor(t *testing.T, dir, host string) {
	t.Helper()

	require.NoError(t, verifyServerCert(t, dir, host), host)
}

func requireFailsFor(t *testing.T, dir, host string) {
	t.Helper()

	require.Error(t, verifyServerCert(t, dir, host), host)
}

func TestGenCertsProducesUsableTLSConfigs(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, runGenCerts(t, dir))

	serverOptions := security.TLSOptions{
		CertFile:     filepath.Join(dir, "server.crt"),
		KeyFile:      filepath.Join(dir, "server.key"),
		ClientCAFile: filepath.Join(dir, "ca.crt"),
	}

	serverConfig, err := serverOptions.ServerConfig()
	require.NoError(t, err)
	require.Len(t, serverConfig.Certificates, 1)
	require.NotNil(t, serverConfig.ClientCAs)

	clientOptions := security.TLSOptions{
		CAFile:     filepath.Join(dir, "ca.crt"),
		CertFile:   filepath.Join(dir, "client.crt"),
		KeyFile:    filepath.Join(dir, "client.key"),
		ServerName: "localhost",
	}

	clientConfig, err := clientOptions.ClientConfig()
	require.NoError(t, err)
	require.Len(t, clientConfig.Certificates, 1)
	require.NotNil(t, clientConfig.RootCAs)
}

func TestGenCertsServerCertificateVerifiesForLocalhost(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, runGenCerts(t, dir))

	requireVerifiesFor(t, dir, "localhost")
	requireVerifiesFor(t, dir, "127.0.0.1")
}

func TestGenCertsHonoursCustomHosts(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, runGenCerts(t, dir, "CERT_HOSTS=fq.internal,10.0.0.7"))

	requireVerifiesFor(t, dir, "fq.internal")
	requireVerifiesFor(t, dir, "10.0.0.7")
	requireFailsFor(t, dir, "localhost")
}

func TestGenCertsRefusesToOverwriteWithoutForce(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, runGenCerts(t, dir))

	before, err := os.ReadFile(filepath.Join(dir, "server.crt"))
	require.NoError(t, err)

	require.Error(t, runGenCerts(t, dir))

	unchanged, err := os.ReadFile(filepath.Join(dir, "server.crt"))
	require.NoError(t, err)
	require.Equal(t, before, unchanged)

	require.NoError(t, runGenCerts(t, dir, "CERT_FORCE=1"))

	after, err := os.ReadFile(filepath.Join(dir, "server.crt"))
	require.NoError(t, err)
	require.NotEqual(t, before, after)
}

func TestGenCertsWritesRestrictivePermissionsOnKeys(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, runGenCerts(t, dir))

	for _, name := range []string{"ca.key", "server.key", "client.key"} {
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm(), name)
	}
}
