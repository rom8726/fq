package in_memory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"fq/internal/database"
)

func TestNewElem(t *testing.T) {
	e := NewElem()

	require.Equal(t, e.ver, database.NoTx)
	require.Equal(t, e.dumpVer, database.NoTx)
}

func TestElem_Incr(t *testing.T) {
	e := NewElem()

	t.Run("no dump tx", func(t *testing.T) {
		curr := e.Incr(1000, database.NoTx)
		require.Equal(t, database.ValueType(1), curr)
		require.Equal(t, database.ValueType(1), e.value)
		require.Equal(t, database.Tx(1000), e.ver)
		require.Equal(t, database.NoTx, e.dumpVer)
		require.Equal(t, database.ValueType(0), e.dumpValue)

		curr = e.Incr(1001, database.NoTx)
		require.Equal(t, database.ValueType(2), curr)
		require.Equal(t, database.ValueType(2), e.value)
		require.Equal(t, database.Tx(1001), e.ver)
		require.Equal(t, database.NoTx, e.dumpVer)
		require.Equal(t, database.ValueType(0), e.dumpValue)
	})

	t.Run("tx = dump tx", func(t *testing.T) {
		curr := e.Incr(1002, 1002)
		require.Equal(t, database.ValueType(3), curr)
		require.Equal(t, database.ValueType(3), e.value)
		require.Equal(t, database.Tx(1002), e.ver)
		require.Equal(t, database.Tx(1002), e.dumpVer)
		require.Equal(t, database.ValueType(3), e.dumpValue)
	})

	t.Run("tx > dump tx", func(t *testing.T) {
		curr := e.Incr(1003, 1002)
		require.Equal(t, database.ValueType(4), curr)
		require.Equal(t, database.ValueType(4), e.value)
		require.Equal(t, database.Tx(1003), e.ver)
		require.Equal(t, database.Tx(1002), e.dumpVer)
		require.Equal(t, database.ValueType(3), e.dumpValue)

		curr = e.Incr(1004, 1003)
		require.Equal(t, database.ValueType(5), curr)
		require.Equal(t, database.ValueType(5), e.value)
		require.Equal(t, database.Tx(1004), e.ver)
		require.Equal(t, database.Tx(1003), e.dumpVer)
		require.Equal(t, database.ValueType(4), e.dumpValue)
	})
}

func TestElem_Value(t *testing.T) {
	e := NewElem()
	e.Incr(1000, database.NoTx)
	require.Equal(t, database.ValueType(1), e.value)
	e.Incr(1000, database.Tx(1000))
	require.Equal(t, database.ValueType(2), e.value)
}

func TestElem_DumpValue(t *testing.T) {
	e := NewElem()
	e.Incr(1000, database.NoTx)
	require.Equal(t, database.ValueType(1), e.DumpValue(1000))
	require.Equal(t, database.ValueType(0), e.DumpValue(999))

	e.Incr(1001, database.Tx(1001))
	require.Equal(t, database.ErrorValue, e.DumpValue(1000))

	require.Equal(t, database.ValueType(2), e.DumpValue(1001))
}
