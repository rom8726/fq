package database

import "errors"

var ErrDumpReadSessionClosed = errors.New("dump read session is closed")

var (
	ErrQuotaLimitMismatch   = errors.New("quota limit mismatch")
	ErrQuotaAlreadyAcquired = errors.New("quota already acquired with different amount")
	ErrQuotaNotEmpty        = errors.New("quota is not empty")
)
