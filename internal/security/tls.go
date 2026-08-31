package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSOptions struct {
	CertFile     string
	KeyFile      string
	ClientCAFile string
	CAFile       string
	ServerName   string
	SkipVerify   bool
	MinVersion   string
}

func (o TLSOptions) Empty() bool {
	return o.CertFile == "" &&
		o.KeyFile == "" &&
		o.ClientCAFile == "" &&
		o.CAFile == "" &&
		o.ServerName == "" &&
		o.MinVersion == "" &&
		!o.SkipVerify
}

func (o TLSOptions) ServerConfig() (*tls.Config, error) {
	if o.Empty() {
		return nil, nil
	}

	if o.CertFile == "" || o.KeyFile == "" {
		return nil, ErrTLSCertRequired
	}

	version, err := tlsMinVersion(o.MinVersion)
	if err != nil {
		return nil, err
	}

	certificate, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   version,
	}

	if o.ClientCAFile != "" {
		pool, err := certPool(o.ClientCAFile)
		if err != nil {
			return nil, err
		}

		config.ClientCAs = pool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return config, nil
}

func (o TLSOptions) ClientConfig() (*tls.Config, error) {
	if o.Empty() {
		return nil, nil
	}

	version, err := tlsMinVersion(o.MinVersion)
	if err != nil {
		return nil, err
	}

	config := &tls.Config{
		MinVersion:         version,
		ServerName:         o.ServerName,
		InsecureSkipVerify: o.SkipVerify, //nolint:gosec // opt-in through the skip_verify setting
	}

	if o.CAFile != "" {
		pool, err := certPool(o.CAFile)
		if err != nil {
			return nil, err
		}

		config.RootCAs = pool
	}

	if o.CertFile != "" || o.KeyFile != "" {
		if o.CertFile == "" || o.KeyFile == "" {
			return nil, ErrTLSKeyPairIncomplete
		}

		certificate, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load key pair: %w", err)
		}

		config.Certificates = []tls.Certificate{certificate}
	}

	return config, nil
}

func tlsMinVersion(value string) (uint16, error) {
	switch value {
	case "", "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrTLSUnknownMinVersion, value)
	}
}

func certPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca file %q: %w", path, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("no certificates found in %q", path)
	}

	return pool, nil
}
