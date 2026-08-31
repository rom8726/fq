package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"time"
)

func NewEphemeralClientCertificate(commonName string) (
	caCert *x509.Certificate,
	clientTLSCert tls.Certificate,
	err error,
) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("generate CA key: %w", err)
	}

	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: commonName + " CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("create CA certificate: %w", err)
	}
	caCert, err = x509.ParseCertificate(caDER)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("parse CA certificate: %w", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("generate client key: %w", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano() + 1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(
		rand.Reader,
		clientTemplate,
		caCert,
		&clientKey.PublicKey,
		caKey,
	)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("create client certificate: %w", err)
	}
	clientCert, err := x509.ParseCertificate(clientDER)
	if err != nil {
		return nil, tls.Certificate{}, fmt.Errorf("parse client certificate: %w", err)
	}

	return caCert, tls.Certificate{
		Certificate: [][]byte{clientDER},
		PrivateKey:  clientKey,
		Leaf:        clientCert,
	}, nil
}
