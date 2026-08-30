package inmemory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
)

func TestQuotaElemAcquireReleaseAndDelete(t *testing.T) {
	table := NewHashTable()
	txCtx := database.TxContext{Tx: 1, CurrTime: 100}

	first, err := table.QuotaAcquire(txCtx, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    4,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipClientLease,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, database.QuotaAcquireResult{
		Acquired:  true,
		Allocated: 4,
		Used:      4,
		Remaining: 6,
		Mutated:   true,
	}, first)

	second, err := table.QuotaAcquire(database.TxContext{Tx: 2, CurrTime: 101}, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    7,
		ClientID:  "client-b",
		Ownership: database.QuotaOwnershipClientLease,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, database.QuotaAcquireResult{
		Acquired:  false,
		Allocated: 0,
		Used:      4,
		Remaining: 6,
	}, second)

	deleted, err := table.QuotaDelete(database.TxContext{Tx: 3, CurrTime: 102}, "pool", nil)
	require.ErrorIs(t, err, database.ErrQuotaNotEmpty)
	require.False(t, deleted)

	released, err := table.QuotaRelease(database.TxContext{Tx: 4, CurrTime: 103}, "pool", "client-a", nil)
	require.NoError(t, err)
	require.True(t, released.Released)

	deleted, err = table.QuotaDelete(database.TxContext{Tx: 5, CurrTime: 104}, "pool", nil)
	require.NoError(t, err)
	require.True(t, deleted)
}

func TestQuotaElemSetLimitAndServerOwnedAcquire(t *testing.T) {
	table := NewHashTable()

	_, err := table.QuotaAcquire(database.TxContext{Tx: 1, CurrTime: 100}, database.QuotaAcquireRequest{
		Name:      "pool",
		Amount:    4,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipServer,
	}, nil)
	require.ErrorIs(t, err, database.ErrQuotaNotFound)

	changed, err := table.QuotaSet(database.TxContext{Tx: 2, CurrTime: 101}, "pool", 10, nil)
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = table.QuotaSet(database.TxContext{Tx: 3, CurrTime: 102}, "pool", 10, nil)
	require.NoError(t, err)
	require.False(t, changed)

	result, err := table.QuotaAcquire(database.TxContext{Tx: 4, CurrTime: 103}, database.QuotaAcquireRequest{
		Name:      "pool",
		Amount:    4,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipServer,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, database.QuotaAcquireResult{
		Acquired:  true,
		Allocated: 4,
		Used:      4,
		Remaining: 6,
		Mutated:   true,
	}, result)

	changed, err = table.QuotaSet(database.TxContext{Tx: 5, CurrTime: 104}, "pool", 3, nil)
	require.ErrorIs(t, err, database.ErrQuotaLimitBelowUsed)
	require.False(t, changed)
}

func TestQuotaElemRejectsMixedOwnership(t *testing.T) {
	table := NewHashTable()

	changed, err := table.QuotaSet(database.TxContext{Tx: 1, CurrTime: 100}, "server-pool", 10, nil)
	require.NoError(t, err)
	require.True(t, changed)

	_, err = table.QuotaAcquire(database.TxContext{Tx: 2, CurrTime: 101}, database.QuotaAcquireRequest{
		Name:      "server-pool",
		Limit:     10,
		Amount:    4,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipClientLease,
	}, nil)
	require.ErrorIs(t, err, database.ErrQuotaOwnershipMismatch)

	_, err = table.QuotaAcquire(database.TxContext{Tx: 3, CurrTime: 102}, database.QuotaAcquireRequest{
		Name:      "lease-pool",
		Limit:     10,
		Amount:    4,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipClientLease,
	}, nil)
	require.NoError(t, err)

	changed, err = table.QuotaSet(database.TxContext{Tx: 4, CurrTime: 103}, "lease-pool", 10, nil)
	require.ErrorIs(t, err, database.ErrQuotaOwnershipMismatch)
	require.False(t, changed)

	_, err = table.QuotaAcquire(database.TxContext{Tx: 5, CurrTime: 104}, database.QuotaAcquireRequest{
		Name:      "lease-pool",
		Amount:    4,
		ClientID:  "client-b",
		Ownership: database.QuotaOwnershipServer,
	}, nil)
	require.ErrorIs(t, err, database.ErrQuotaOwnershipMismatch)
}

func TestQuotaElemAcquireIsIdempotentForSameClientAndAmount(t *testing.T) {
	table := NewHashTable()
	writes := 0
	beforeApply := func() error {
		writes++

		return nil
	}

	request := database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    4,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipClientLease,
		ExpiresAt: 160,
	}
	first, err := table.QuotaAcquire(database.TxContext{Tx: 1, CurrTime: 100}, request, beforeApply)
	require.NoError(t, err)
	require.True(t, first.Acquired)
	require.Equal(t, uint32(60), first.ExpiresAfter)

	second, err := table.QuotaAcquire(database.TxContext{Tx: 2, CurrTime: 110}, request, beforeApply)
	require.NoError(t, err)
	require.Equal(t, database.QuotaAcquireResult{
		Acquired:     true,
		Allocated:    4,
		Used:         4,
		Remaining:    6,
		ExpiresAfter: 50,
	}, second)
	require.Equal(t, 1, writes)

	_, err = table.QuotaAcquire(database.TxContext{Tx: 3, CurrTime: 111}, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    5,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipClientLease,
	}, beforeApply)
	require.ErrorIs(t, err, database.ErrQuotaAlreadyAcquired)
	require.Equal(t, 1, writes)
}

func TestQuotaElemExpiresClientAllocation(t *testing.T) {
	table := NewHashTable()

	_, err := table.QuotaAcquire(database.TxContext{Tx: 1, CurrTime: 100}, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    8,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipClientLease,
		ExpiresAt: 110,
	}, nil)
	require.NoError(t, err)

	result, err := table.QuotaAcquire(database.TxContext{Tx: 2, CurrTime: 110}, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    8,
		ClientID:  "client-b",
		Ownership: database.QuotaOwnershipClientLease,
	}, nil)
	require.NoError(t, err)
	require.True(t, result.Acquired)
	require.Equal(t, database.ValueType(8), result.Used)
	require.Equal(t, database.ValueType(2), result.Remaining)
}

func TestQuotaElemInfoReturnsSortedActiveClients(t *testing.T) {
	table := NewHashTable()

	_, err := table.QuotaAcquire(database.TxContext{Tx: 1, CurrTime: 100}, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    3,
		ClientID:  "client-b",
		Ownership: database.QuotaOwnershipClientLease,
		ExpiresAt: 160,
	}, nil)
	require.NoError(t, err)
	_, err = table.QuotaAcquire(database.TxContext{Tx: 2, CurrTime: 101}, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    4,
		ClientID:  "client-a",
		Ownership: database.QuotaOwnershipClientLease,
	}, nil)
	require.NoError(t, err)
	_, err = table.QuotaAcquire(database.TxContext{Tx: 3, CurrTime: 102}, database.QuotaAcquireRequest{
		Name:      "pool",
		Limit:     10,
		Amount:    2,
		ClientID:  "client-expired",
		Ownership: database.QuotaOwnershipClientLease,
		ExpiresAt: 109,
	}, nil)
	require.NoError(t, err)

	info := table.QuotaInfo(110, "pool")

	require.Equal(t, database.QuotaInfo{
		Limit:     10,
		Used:      7,
		Remaining: 3,
		Clients: []database.QuotaClientInfo{
			{ClientID: "client-a", Amount: 4},
			{ClientID: "client-b", Amount: 3, ExpiresAt: 160},
		},
	}, info)
}
