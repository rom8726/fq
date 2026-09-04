package database

import "github.com/fq-db/fq/internal/protocol"

var ErrDumpReadSessionClosed = protocol.NewError(protocol.CodeInternal, "dump read session is closed")

var (
	ErrQuotaLimitMismatch = protocol.NewError(
		protocol.CodeQuotaLimitMismatch, "quota limit mismatch")
	ErrQuotaAlreadyAcquired = protocol.NewError(
		protocol.CodeQuotaAlreadyAcquired,
		"quota already acquired with different amount")
	ErrQuotaNotEmpty = protocol.NewError(
		protocol.CodeQuotaNotEmpty, "quota is not empty")
	ErrQuotaNotFound = protocol.NewError(
		protocol.CodeQuotaNotFound, "quota not found")
	ErrQuotaLimitBelowUsed = protocol.NewError(
		protocol.CodeQuotaLimitBelowUsed, "quota limit is below used amount")
	ErrQuotaOwnershipMismatch = protocol.NewError(
		protocol.CodeQuotaOwnershipMismatch, "quota ownership mismatch")
	ErrQuotaPolicyMismatch = protocol.NewError(
		protocol.CodeQuotaPolicyMismatch, "quota policy mismatch")
	ErrInvalidScanCursor = protocol.NewError(
		protocol.CodeInvalidScanCursor, "invalid scan cursor")
	ErrScanIndexDisabled = protocol.NewError(
		protocol.CodeScanIndexDisabled, "scan index is disabled")
	ErrReadOnlyReplica = protocol.NewError(
		protocol.CodeReadOnlyReplica, "instance is a read-only replica")
)
