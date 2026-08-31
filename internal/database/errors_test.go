package database

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
)

func TestDatabaseErrorsCarryCodes(t *testing.T) {
	cases := []struct {
		err  error
		code protocol.Code
	}{
		{ErrQuotaNotFound, protocol.CodeQuotaNotFound},
		{ErrQuotaLimitMismatch, protocol.CodeQuotaLimitMismatch},
		{ErrQuotaAlreadyAcquired, protocol.CodeQuotaAlreadyAcquired},
		{ErrQuotaNotEmpty, protocol.CodeQuotaNotEmpty},
		{ErrQuotaLimitBelowUsed, protocol.CodeQuotaLimitBelowUsed},
		{ErrQuotaOwnershipMismatch, protocol.CodeQuotaOwnershipMismatch},
		{ErrQuotaPolicyMismatch, protocol.CodeQuotaPolicyMismatch},
		{ErrInvalidScanCursor, protocol.CodeInvalidScanCursor},
		{ErrScanIndexDisabled, protocol.CodeScanIndexDisabled},
		{errKeyEmpty, protocol.CodeKeyEmpty},
		{errKeyTooLong, protocol.CodeKeyTooLong},
		{errBatchSizeNotNumber, protocol.CodeBatchSizeNotNumber},
		{errInvalidBatchSize, protocol.CodeInvalidBatchSize},
		{errLimitNotNumber, protocol.CodeLimitNotNumber},
		{errInvalidLimit, protocol.CodeInvalidLimit},
		{errInvalidRLimitAlgo, protocol.CodeInvalidRLimitAlgo},
		{errInvalidScanCount, protocol.CodeInvalidScanCount},
		{errInvalidArgumentsCount, protocol.CodeInvalidArgumentsCount},
		{errInternalConfiguration, protocol.CodeInternalConfiguration},
		{errInspectUnavailable, protocol.CodeInspectUnavailable},
		{errInspectReportTooLarge, protocol.CodeInspectReportTooLarge},
		{errMessageSizeTooSmall, protocol.CodeMessageSizeTooSmall},
	}

	for _, tc := range cases {
		code, ok := protocol.CodeOf(tc.err)
		require.True(t, ok, tc.err.Error())
		require.Equal(t, tc.code, code, tc.err.Error())
	}
}

func TestWrappedDatabaseErrorKeepsCode(t *testing.T) {
	_, err := makeLimit("0")
	require.Error(t, err)

	code, ok := protocol.CodeOf(err)
	require.True(t, ok)
	require.Equal(t, protocol.CodeInvalidLimit, code)
}
