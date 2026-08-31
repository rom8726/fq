package security

import "errors"

var (
	ErrUnknownRole           = errors.New("unknown role")
	ErrDuplicateToken        = errors.New("duplicate token")
	ErrSecretSourceAmbiguous = errors.New("exactly one of token_env and token_file must be set")
	ErrSecretEmpty           = errors.New("secret is empty")
	ErrSecretTooShort        = errors.New("secret is too short")
	ErrNotAuthenticated      = errors.New("not authenticated")
	ErrPermissionDenied      = errors.New("permission denied")
	ErrAuthenticationFailed  = errors.New("authentication failed")
	ErrTooManyAuthFailures   = errors.New("too many authentication failures")
	ErrTLSKeyPairIncomplete  = errors.New("cert_file and key_file must be set together")
	ErrTLSCertRequired       = errors.New("cert_file and key_file are required")
	ErrTLSUnknownMinVersion  = errors.New("unknown tls min version")
)
