package protocol

type Code uint16

const (
	CodeInvalidSymbol            Code = 1000
	CodeInvalidCommand           Code = 1001
	CodeInvalidArguments         Code = 1002
	CodeInvalidArgumentsCount    Code = 1003
	CodeMessageTooLarge          Code = 1004
	CodeHandshakeRequired        Code = 1010
	CodeUnsupportedVersion       Code = 1011
	CodeVersionAlreadyNegotiated Code = 1012

	CodeKeyEmpty           Code = 2000
	CodeKeyTooLong         Code = 2001
	CodeBatchSizeNotNumber Code = 2002
	CodeInvalidBatchSize   Code = 2003
	CodeLimitNotNumber     Code = 2004
	CodeInvalidLimit       Code = 2005
	CodeInvalidRLimitAlgo  Code = 2006
	CodeInvalidScanCount   Code = 2007
	CodeInvalidScanCursor  Code = 2008

	CodeNotAuthenticated     Code = 3000
	CodePermissionDenied     Code = 3001
	CodeAuthenticationFailed Code = 3002
	CodeTooManyAuthFailures  Code = 3003

	CodeQuotaNotFound          Code = 4000
	CodeQuotaLimitMismatch     Code = 4001
	CodeQuotaAlreadyAcquired   Code = 4002
	CodeQuotaNotEmpty          Code = 4003
	CodeQuotaLimitBelowUsed    Code = 4004
	CodeQuotaOwnershipMismatch Code = 4005
	CodeQuotaPolicyMismatch    Code = 4006

	CodeScanIndexDisabled     Code = 5000
	CodeInspectUnavailable    Code = 5001
	CodeInspectReportTooLarge Code = 5002
	CodeMessageSizeTooSmall   Code = 5003

	CodeUnsupportedCompression Code = 5004
	CodeReadOnlyReplica        Code = 5005

	CodeInternal              Code = 9000
	CodeInternalConfiguration Code = 9001
)

type CodeInfo struct {
	Code    Code
	Name    string
	Message string
}

var codes = []CodeInfo{
	{CodeInvalidSymbol, "CodeInvalidSymbol", "invalid symbol"},
	{CodeInvalidCommand, "CodeInvalidCommand", "invalid command"},
	{CodeInvalidArguments, "CodeInvalidArguments", "invalid arguments"},
	{CodeInvalidArgumentsCount, "CodeInvalidArgumentsCount", "invalid arguments count"},
	{CodeMessageTooLarge, "CodeMessageTooLarge", "message size exceeds maximum"},
	{CodeHandshakeRequired, "CodeHandshakeRequired", "handshake required"},
	{CodeUnsupportedVersion, "CodeUnsupportedVersion", "unsupported protocol version"},
	{CodeVersionAlreadyNegotiated, "CodeVersionAlreadyNegotiated", "protocol version already negotiated"},
	{CodeKeyEmpty, "CodeKeyEmpty", "key cannot be empty"},
	{CodeKeyTooLong, "CodeKeyTooLong", "key length exceeds maximum"},
	{CodeBatchSizeNotNumber, "CodeBatchSizeNotNumber", "batch is not a number"},
	{CodeInvalidBatchSize, "CodeInvalidBatchSize", "invalid batch size"},
	{CodeLimitNotNumber, "CodeLimitNotNumber", "limit is not a number"},
	{CodeInvalidLimit, "CodeInvalidLimit", "invalid limit"},
	{CodeInvalidRLimitAlgo, "CodeInvalidRLimitAlgo", "invalid rate limit algorithm"},
	{CodeInvalidScanCount, "CodeInvalidScanCount", "invalid scan count"},
	{CodeInvalidScanCursor, "CodeInvalidScanCursor", "invalid scan cursor"},
	{CodeNotAuthenticated, "CodeNotAuthenticated", "not authenticated"},
	{CodePermissionDenied, "CodePermissionDenied", "permission denied"},
	{CodeAuthenticationFailed, "CodeAuthenticationFailed", "authentication failed"},
	{CodeTooManyAuthFailures, "CodeTooManyAuthFailures", "too many authentication failures"},
	{CodeQuotaNotFound, "CodeQuotaNotFound", "quota not found"},
	{CodeQuotaLimitMismatch, "CodeQuotaLimitMismatch", "quota limit mismatch"},
	{CodeQuotaAlreadyAcquired, "CodeQuotaAlreadyAcquired", "quota already acquired with different amount"},
	{CodeQuotaNotEmpty, "CodeQuotaNotEmpty", "quota is not empty"},
	{CodeQuotaLimitBelowUsed, "CodeQuotaLimitBelowUsed", "quota limit is below used amount"},
	{CodeQuotaOwnershipMismatch, "CodeQuotaOwnershipMismatch", "quota ownership mismatch"},
	{CodeQuotaPolicyMismatch, "CodeQuotaPolicyMismatch", "quota policy mismatch"},
	{CodeScanIndexDisabled, "CodeScanIndexDisabled", "scan index is disabled"},
	{CodeInspectUnavailable, "CodeInspectUnavailable", "inspect is not available"},
	{CodeInspectReportTooLarge, "CodeInspectReportTooLarge", "inspect report too large"},
	{CodeMessageSizeTooSmall, "CodeMessageSizeTooSmall", "max message size too small for a chunked response"},
	{
		CodeUnsupportedCompression,
		"CodeUnsupportedCompression",
		"replica does not support the configured compression codec",
	},
	{CodeReadOnlyReplica, "CodeReadOnlyReplica", "instance is a read-only replica"},
	{CodeInternal, "CodeInternal", "internal error"},
	{CodeInternalConfiguration, "CodeInternalConfiguration", "internal configuration error"},
}

func AllCodes() []CodeInfo {
	result := make([]CodeInfo, len(codes))
	copy(result, codes)

	return result
}

func (c Code) Category() int {
	return int(c) / 1000
}

func (c Code) String() string {
	for i := range codes {
		if codes[i].Code == c {
			return codes[i].Message
		}
	}

	return CodeInternal.String()
}
