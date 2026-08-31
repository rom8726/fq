package security

import (
	"errors"

	"github.com/fq-db/fq/internal/protocol"
)

var (
	ErrUnknownRole           = errors.New("unknown role")
	ErrDuplicateToken        = errors.New("duplicate token")
	ErrSecretSourceAmbiguous = errors.New("exactly one of token_env and token_file must be set")
	ErrSecretEmpty           = errors.New("secret is empty")
	ErrSecretTooShort        = errors.New("secret is too short")
	ErrNotAuthenticated      = protocol.NewError(protocol.CodeNotAuthenticated, "not authenticated")
	ErrPermissionDenied      = protocol.NewError(protocol.CodePermissionDenied, "permission denied")
	ErrAuthenticationFailed  = protocol.NewError(protocol.CodeAuthenticationFailed, "authentication failed")
	ErrTooManyAuthFailures   = protocol.NewError(protocol.CodeTooManyAuthFailures, "too many authentication failures")
	ErrTLSKeyPairIncomplete  = errors.New("cert_file and key_file must be set together")
	ErrTLSCertRequired       = errors.New("cert_file and key_file are required")
	ErrTLSUnknownMinVersion  = errors.New("unknown tls min version")
)
