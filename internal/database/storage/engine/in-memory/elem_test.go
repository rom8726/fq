package inmemory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"fq/internal/database"
)

func TestNewElem(t *testing.T) {
	e := NewFqElem(60)

	require.Equal(t, e.batchSize, database.TxTime(60))
	require.Equal(t, e.ver, database.NoTx)
	require.Equal(t, e.dumpVer, database.NoTx)
}

func TestElem_Incr(t *testing.T) {
	e := NewFqElem(60)
	currTime := database.TxTime(time.Now().Unix())

	t.Run("no dump tx", func(t *testing.T) {
		curr := e.Incr(database.TxContext{Tx: 1000, DumpTx: database.NoTx, CurrTime: currTime})
		require.Equal(t, database.ValueType(1), curr)
		require.Equal(t, database.ValueType(1), e.value)
		require.Equal(t, database.Tx(1000), e.ver)
		require.Equal(t, database.NoTx, e.dumpVer)
		require.Equal(t, database.ValueType(0), e.dumpValue)

		curr = e.Incr(database.TxContext{Tx: 1001, DumpTx: database.NoTx, CurrTime: currTime})
		require.Equal(t, database.ValueType(2), curr)
		require.Equal(t, database.ValueType(2), e.value)
		require.Equal(t, database.Tx(1001), e.ver)
		require.Equal(t, database.NoTx, e.dumpVer)
		require.Equal(t, database.ValueType(0), e.dumpValue)
	})

	t.Run("tx = dump tx", func(t *testing.T) {
		curr := e.Incr(database.TxContext{Tx: 1002, DumpTx: 1002, CurrTime: currTime})
		require.Equal(t, database.ValueType(3), curr)
		require.Equal(t, database.ValueType(3), e.value)
		require.Equal(t, database.Tx(1002), e.ver)
		require.Equal(t, database.Tx(1002), e.dumpVer)
		require.Equal(t, database.ValueType(3), e.dumpValue)
	})

	t.Run("tx > dump tx", func(t *testing.T) {
		curr := e.Incr(database.TxContext{Tx: 1003, DumpTx: 1002, CurrTime: currTime})
		require.Equal(t, database.ValueType(4), curr)
		require.Equal(t, database.ValueType(4), e.value)
		require.Equal(t, database.Tx(1003), e.ver)
		require.Equal(t, database.Tx(1002), e.dumpVer)
		require.Equal(t, database.ValueType(3), e.dumpValue)

		curr = e.Incr(database.TxContext{Tx: 1004, DumpTx: 1003, CurrTime: currTime})
		require.Equal(t, database.ValueType(5), curr)
		require.Equal(t, database.ValueType(5), e.value)
		require.Equal(t, database.Tx(1004), e.ver)
		require.Equal(t, database.Tx(1003), e.dumpVer)
		require.Equal(t, database.ValueType(4), e.dumpValue)
	})

	t.Run("current batch changed", func(t *testing.T) {
		e := NewFqElem(1)
		curr := e.Incr(database.TxContext{Tx: 1000, DumpTx: database.NoTx, CurrTime: database.TxTime(time.Now().Unix())})
		require.Equal(t, database.ValueType(1), curr)
		curr = e.Incr(database.TxContext{Tx: 1001, DumpTx: database.NoTx, CurrTime: database.TxTime(time.Now().Unix())})
		require.Equal(t, database.ValueType(2), curr)

		time.Sleep(time.Millisecond * 1200)
		curr = e.Incr(database.TxContext{Tx: 1002, DumpTx: database.NoTx, CurrTime: database.TxTime(time.Now().Unix())})
		require.Equal(t, database.ValueType(1), curr)
	})
}

func TestElem_Value(t *testing.T) {
	e := NewFqElem(60)
	e.Incr(database.TxContext{Tx: 1000, DumpTx: database.NoTx})
	require.Equal(t, database.ValueType(1), e.value)
	e.Incr(database.TxContext{Tx: 1000, DumpTx: database.Tx(1000)})
	require.Equal(t, database.ValueType(2), e.value)
}

func TestElem_DumpValue(t *testing.T) {
	now := database.TxTime(time.Now().Unix())

	e := NewFqElem(60)
	e.Incr(database.TxContext{Tx: 1000, DumpTx: database.NoTx, CurrTime: now})
	v, lastTime := e.DumpValue(1000)
	require.Equal(t, database.ValueType(1), v)
	require.Equal(t, now, lastTime)

	v, lastTime = e.DumpValue(999)
	require.Equal(t, database.ValueType(0), v)
	require.Equal(t, database.TxTime(0), lastTime)

	e.Incr(database.TxContext{Tx: 1001, DumpTx: database.Tx(1001), CurrTime: now})
	v, lastTime = e.DumpValue(1000)
	require.Equal(t, database.ErrorValue, v)
	require.Equal(t, database.TxTime(0), lastTime)

	v, lastTime = e.DumpValue(1001)
	require.Equal(t, database.ValueType(2), v)
	require.Equal(t, now, lastTime)
}
