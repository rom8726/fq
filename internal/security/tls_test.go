package security_test

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

func TestTLSOptionsEmptyProducesNilConfigs(t *testing.T) {
	options := security.TLSOptions{}

	require.True(t, options.Empty())

	serverConfig, err := options.ServerConfig()
	require.NoError(t, err)
	require.Nil(t, serverConfig)

	clientConfig, err := options.ClientConfig()
	require.NoError(t, err)
	require.Nil(t, clientConfig)
}

func TestTLSServerConfig(t *testing.T) {
	pki := newTestPKI(t)
	options := security.TLSOptions{CertFile: pki.ServerCertFile, KeyFile: pki.ServerKeyFile}

	config, err := options.ServerConfig()
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
	require.Equal(t, uint16(tls.VersionTLS12), config.MinVersion)
	require.Equal(t, tls.NoClientCert, config.ClientAuth)
}

func TestTLSServerConfigEnablesMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	options := security.TLSOptions{
		CertFile:     pki.ServerCertFile,
		KeyFile:      pki.ServerKeyFile,
		ClientCAFile: pki.CAFile,
		MinVersion:   "1.3",
	}

	config, err := options.ServerConfig()
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, config.ClientAuth)
	require.NotNil(t, config.ClientCAs)
	require.Equal(t, uint16(tls.VersionTLS13), config.MinVersion)
}

func TestTLSServerConfigRequiresKeyPair(t *testing.T) {
	pki := newTestPKI(t)

	_, err := security.TLSOptions{CertFile: pki.ServerCertFile}.ServerConfig()
	require.ErrorIs(t, err, security.ErrTLSCertRequired)

	_, err = security.TLSOptions{ClientCAFile: pki.CAFile}.ServerConfig()
	require.ErrorIs(t, err, security.ErrTLSCertRequired)
}

func TestTLSClientConfig(t *testing.T) {
	pki := newTestPKI(t)
	options := security.TLSOptions{CAFile: pki.CAFile, ServerName: "localhost"}

	config, err := options.ClientConfig()
	require.NoError(t, err)
	require.NotNil(t, config.RootCAs)
	require.Equal(t, "localhost", config.ServerName)
	require.False(t, config.InsecureSkipVerify)
	require.Empty(t, config.Certificates)
}

func TestTLSClientConfigWithClientCertificate(t *testing.T) {
	pki := newTestPKI(t)
	options := security.TLSOptions{
		CAFile:   pki.CAFile,
		CertFile: pki.ClientCertFile,
		KeyFile:  pki.ClientKeyFile,
	}

	config, err := options.ClientConfig()
	require.NoError(t, err)
	require.Len(t, config.Certificates, 1)
}

func TestTLSClientConfigRejectsHalfKeyPair(t *testing.T) {
	pki := newTestPKI(t)

	_, err := security.TLSOptions{CAFile: pki.CAFile, CertFile: pki.ClientCertFile}.ClientConfig()
	require.ErrorIs(t, err, security.ErrTLSKeyPairIncomplete)
}

func TestTLSRejectsUnknownMinVersion(t *testing.T) {
	pki := newTestPKI(t)
	options := security.TLSOptions{
		CertFile:   pki.ServerCertFile,
		KeyFile:    pki.ServerKeyFile,
		MinVersion: "1.1",
	}

	_, err := options.ServerConfig()
	require.ErrorIs(t, err, security.ErrTLSUnknownMinVersion)
}

func TestTLSSkipVerifyIsHonoured(t *testing.T) {
	config, err := security.TLSOptions{SkipVerify: true}.ClientConfig()
	require.NoError(t, err)
	require.True(t, config.InsecureSkipVerify)
}
