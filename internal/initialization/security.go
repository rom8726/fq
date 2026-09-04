package initialization

import (
	"github.com/rs/zerolog"
)

const (
	clientPortName      = "client"
	replicationPortName = "replication"
)

func warnCleartextAuth(logger *zerolog.Logger, port, address string, authEnabled, tlsEnabled bool) {
	if !authEnabled || tlsEnabled {
		return
	}

	logger.Warn().
		Str("port", port).
		Str("address", address).
		Msg("authentication is enabled without tls: tokens are transmitted in cleartext")
}
