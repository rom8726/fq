package database

const (
	ErrorValue ValueType = -1

	NoTx Tx = 0

	DumpElemKindCounter             uint32 = 0
	DumpElemKindSlidingWindowBucket uint32 = 1
	DumpElemKindTokenBucket         uint32 = 2
	DumpElemKindQuotaAllocation     uint32 = 3
	DumpElemKindQuotaConfig         uint32 = 4
)

type ValueType int32

type QuotaOwnership uint32

const (
	QuotaOwnershipUnknown QuotaOwnership = iota
	QuotaOwnershipServer
	QuotaOwnershipClientLease
)

type QuotaPolicy uint32

const (
	QuotaPolicyUnknown QuotaPolicy = iota
	QuotaPolicyFixed
	QuotaPolicyPerClient
)

type Tx uint64

type TxTime uint32

type TxContext struct {
	Tx       Tx
	DumpTx   Tx
	CurrTime TxTime
	FromWAL  bool
}

type BatchKey struct {
	BatchSize    uint32
	BatchSizeStr string
	Key          string
}

type DumpElem struct {
	Kind      uint32
	Key       string
	BatchSize uint32
	Limit     ValueType
	Value     ValueType
	Ownership QuotaOwnership
	Policy    QuotaPolicy
	Clients   uint32
	ClientID  string
	ExpiresAt TxTime
	TxAt      TxTime
	Tx        Tx
}

type DumpChunk struct {
	Elems   []DumpElem
	Applied chan error
}

type RateLimitResult struct {
	Allowed     bool
	Current     ValueType
	Remaining   ValueType
	ResetAfter  uint32
	LimitFilled bool
}

type LimitEvent struct {
	Key        string
	Window     uint32
	Current    ValueType
	ResetAfter uint32
}

type QuotaAcquireRequest struct {
	Name      string
	Limit     ValueType
	Amount    ValueType
	ClientID  string
	Ownership QuotaOwnership
	Policy    QuotaPolicy
	TTL       uint32
	ExpiresAt TxTime
}

type QuotaSetRequest struct {
	Name    string
	Limit   ValueType
	Policy  QuotaPolicy
	Clients uint32
}

type QuotaAcquireResult struct {
	Acquired     bool
	Allocated    ValueType
	Used         ValueType
	Remaining    ValueType
	ExpiresAfter uint32
	Mutated      bool
}

type QuotaClientInfo struct {
	ClientID  string
	Amount    ValueType
	ExpiresAt TxTime
}

type QuotaInfo struct {
	Limit     ValueType
	Used      ValueType
	Remaining ValueType
	Clients   []QuotaClientInfo
}

type QuotaReleaseResult struct {
	Released  bool
	Amount    ValueType
	Used      ValueType
	Remaining ValueType
	ExpiresAt TxTime
}

type QuotaEvent struct {
	Event     string
	Name      string
	ClientID  string
	Amount    ValueType
	Used      ValueType
	Remaining ValueType
	ExpiresAt TxTime
}
