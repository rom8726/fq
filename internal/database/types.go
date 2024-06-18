package database

const (
	ErrorValue ValueType = -1

	NoTx Tx = 0
)

type ValueType int32

type Tx uint64

type TxTime uint32

type TxContext struct {
	Tx     Tx
	DumpTx Tx
}

type BatchKey struct {
	BatchSize    uint32
	BatchSizeStr string
	Key          string
}
