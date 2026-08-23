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

func TestElem_RLimitFixedWindow(t *testing.T) {
	e := NewFqElem(60)
	currTime := database.TxTime(120)
	beforeApplyCalls := 0
	beforeApply := func() error {
		beforeApplyCalls++

		return nil
	}

	first, err := e.RLimitFixedWindow(
		database.TxContext{Tx: 1000, DumpTx: database.NoTx, CurrTime: currTime},
		2,
		beforeApply,
	)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    true,
		Current:    1,
		Remaining:  1,
		ResetAfter: 60,
	}, first)

	second, err := e.RLimitFixedWindow(
		database.TxContext{Tx: 1001, DumpTx: database.NoTx, CurrTime: currTime + 1},
		2,
		beforeApply,
	)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:     true,
		Current:     2,
		Remaining:   0,
		ResetAfter:  59,
		LimitFilled: true,
	}, second)

	denied, err := e.RLimitFixedWindow(
		database.TxContext{Tx: 1002, DumpTx: database.NoTx, CurrTime: currTime + 2},
		2,
		beforeApply,
	)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    false,
		Current:    2,
		Remaining:  0,
		ResetAfter: 58,
	}, denied)
	require.Equal(t, 2, beforeApplyCalls)

	nextWindow, err := e.RLimitFixedWindow(
		database.TxContext{Tx: 1003, DumpTx: database.NoTx, CurrTime: currTime + 60},
		2,
		beforeApply,
	)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    true,
		Current:    1,
		Remaining:  1,
		ResetAfter: 60,
	}, nextWindow)
	require.Equal(t, 3, beforeApplyCalls)
}

func TestSlidingWindowElem_RLimit(t *testing.T) {
	e := NewSlidingWindowElem(60)
	beforeApplyCalls := 0
	beforeApply := func() error {
		beforeApplyCalls++

		return nil
	}

	first, err := e.RLimit(database.TxContext{Tx: 1000, CurrTime: 100}, 2, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    true,
		Current:    1,
		Remaining:  1,
		ResetAfter: 60,
	}, first)

	second, err := e.RLimit(database.TxContext{Tx: 1001, CurrTime: 110}, 2, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:     true,
		Current:     2,
		Remaining:   0,
		ResetAfter:  50,
		LimitFilled: true,
	}, second)

	denied, err := e.RLimit(database.TxContext{Tx: 1002, CurrTime: 120}, 2, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    false,
		Current:    2,
		Remaining:  0,
		ResetAfter: 40,
	}, denied)
	require.Equal(t, 2, beforeApplyCalls)

	afterOldestExpires, err := e.RLimit(database.TxContext{Tx: 1003, CurrTime: 160}, 2, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:     true,
		Current:     2,
		Remaining:   0,
		ResetAfter:  10,
		LimitFilled: true,
	}, afterOldestExpires)
	require.Equal(t, 3, beforeApplyCalls)
}

func TestSlidingWindowElem_DumpRespectsDumpTxInsideBucket(t *testing.T) {
	e := NewSlidingWindowElem(60)
	now := database.TxTime(time.Now().Unix())
	e.AddEvent(database.TxContext{Tx: 1, CurrTime: now})
	e.AddEvent(database.TxContext{Tx: 2, CurrTime: now})

	ch := make(chan database.DumpElem, 1)
	e.Dump(nil, hashTableKey{key: "key", batchSize: 60}, 1, ch)

	require.Len(t, ch, 1)
	elem := <-ch
	require.Equal(t, database.DumpElemKindSlidingWindowBucket, elem.Kind)
	require.Equal(t, database.ValueType(1), elem.Value)
	require.Equal(t, database.Tx(1), elem.Tx)
	require.Equal(t, now, elem.TxAt)
}

func TestTokenBucketElem_RLimit(t *testing.T) {
	e := NewTokenBucketElem(10)
	beforeApplyCalls := 0
	beforeApply := func() error {
		beforeApplyCalls++

		return nil
	}

	first, err := e.RLimit(database.TxContext{Tx: 1, CurrTime: 100}, 3, 1, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    true,
		Current:    1,
		Remaining:  2,
		ResetAfter: 0,
	}, first)

	second, err := e.RLimit(database.TxContext{Tx: 2, CurrTime: 100}, 3, 1, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    true,
		Current:    2,
		Remaining:  1,
		ResetAfter: 0,
	}, second)

	third, err := e.RLimit(database.TxContext{Tx: 3, CurrTime: 100}, 3, 1, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:     true,
		Current:     3,
		Remaining:   0,
		ResetAfter:  10,
		LimitFilled: true,
	}, third)

	denied, err := e.RLimit(database.TxContext{Tx: 4, CurrTime: 105}, 3, 1, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:    false,
		Current:    3,
		Remaining:  0,
		ResetAfter: 5,
	}, denied)

	refilled, err := e.RLimit(database.TxContext{Tx: 5, CurrTime: 110}, 3, 1, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:     true,
		Current:     3,
		Remaining:   0,
		ResetAfter:  10,
		LimitFilled: true,
	}, refilled)
	require.Equal(t, 4, beforeApplyCalls)
}

func TestTokenBucketElem_DumpAndRestore(t *testing.T) {
	e := NewTokenBucketElem(10)
	_, err := e.RLimit(database.TxContext{Tx: 1, CurrTime: 100}, 3, 1, nil)
	require.NoError(t, err)
	_, err = e.RLimit(database.TxContext{Tx: 2, CurrTime: 100}, 3, 1, nil)
	require.NoError(t, err)

	value, txAt, tx := e.DumpValue(2)
	require.Equal(t, database.ValueType(1), value)
	require.Equal(t, database.TxTime(100), txAt)
	require.Equal(t, database.Tx(2), tx)

	restored := NewTokenBucketElem(10)
	restored.Restore(database.DumpElem{
		Kind:      database.DumpElemKindTokenBucket,
		Value:     value,
		TxAt:      txAt,
		Tx:        tx,
		BatchSize: 10,
	})

	result, err := restored.RLimit(database.TxContext{Tx: 3, CurrTime: 100}, 3, 1, nil)
	require.NoError(t, err)
	require.Equal(t, database.RateLimitResult{
		Allowed:     true,
		Current:     3,
		Remaining:   0,
		ResetAfter:  10,
		LimitFilled: true,
	}, result)
}

func TestElem_DumpValue(t *testing.T) {
	now := database.TxTime(time.Now().Unix())

	e := NewFqElem(60)
	e.Incr(database.TxContext{Tx: 1000, DumpTx: database.NoTx, CurrTime: now})
	v, lastTime, tx := e.DumpValue(1000)
	require.Equal(t, database.ValueType(1), v)
	require.Equal(t, now, lastTime)
	require.Equal(t, database.Tx(1000), tx)

	v, lastTime, tx = e.DumpValue(999)
	require.Equal(t, database.ValueType(0), v)
	require.Equal(t, database.TxTime(0), lastTime)
	require.Equal(t, database.Tx(0), tx)

	e.Incr(database.TxContext{Tx: 1001, DumpTx: database.Tx(1001), CurrTime: now})
	v, lastTime, tx = e.DumpValue(1000)
	require.Equal(t, database.ErrorValue, v)
	require.Equal(t, database.TxTime(0), lastTime)
	require.Equal(t, database.Tx(0), tx)

	v, lastTime, tx = e.DumpValue(1001)
	require.Equal(t, database.ValueType(2), v)
	require.Equal(t, now, lastTime)
	require.Equal(t, database.Tx(1001), tx)
}
