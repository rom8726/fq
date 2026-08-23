package database

const (
	ErrorValue ValueType = -1

	NoTx Tx = 0

	DumpElemKindCounter             uint32 = 0
	DumpElemKindSlidingWindowBucket uint32 = 1
	DumpElemKindTokenBucket         uint32 = 2
)

type ValueType int32

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
	Value     ValueType
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
