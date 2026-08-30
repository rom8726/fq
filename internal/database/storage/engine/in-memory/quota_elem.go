package inmemory

import (
	"fmt"
	"sort"
	"sync"

	"github.com/fq-db/fq/internal/database"
)

type quotaAllocation struct {
	amount    database.ValueType
	expiresAt database.TxTime
	txAt      database.TxTime
	tx        database.Tx
}

type quotaDumpAllocation struct {
	clientID string
	quotaAllocation
}

type QuotaElem struct {
	limit       database.ValueType
	ownership   database.QuotaOwnership
	used        database.ValueType
	allocations map[string]quotaAllocation
	ver         database.Tx
	mu          sync.RWMutex
}

func NewQuotaElem(limit database.ValueType, ownership database.QuotaOwnership) *QuotaElem {
	return &QuotaElem{
		limit:       limit,
		ownership:   ownership,
		allocations: make(map[string]quotaAllocation),
		ver:         database.NoTx,
	}
}

func (e *QuotaElem) SetLimit(
	txCtx database.TxContext,
	limit database.ValueType,
	beforeApply func() error,
) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(txCtx.CurrTime)
	if e.ownership == database.QuotaOwnershipUnknown {
		e.ownership = database.QuotaOwnershipServer
	}
	if e.ownership != database.QuotaOwnershipServer {
		return false, database.ErrQuotaOwnershipMismatch
	}
	if e.used > limit {
		return false, database.ErrQuotaLimitBelowUsed
	}
	if e.limit == limit {
		return false, nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return false, fmt.Errorf("before apply quota set: %w", err)
		}
	}

	e.limit = limit
	e.ownership = database.QuotaOwnershipServer
	e.ver = txCtx.Tx

	return true, nil
}

func (e *QuotaElem) Acquire(
	txCtx database.TxContext,
	request database.QuotaAcquireRequest,
	beforeApply func() error,
) (database.QuotaAcquireResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(txCtx.CurrTime)
	if request.Ownership == database.QuotaOwnershipUnknown {
		request.Ownership = database.QuotaOwnershipClientLease
	}
	if e.ownership == database.QuotaOwnershipUnknown {
		e.ownership = request.Ownership
	}
	if e.ownership != request.Ownership {
		return database.QuotaAcquireResult{}, database.ErrQuotaOwnershipMismatch
	}
	if e.limit != request.Limit {
		return database.QuotaAcquireResult{}, database.ErrQuotaLimitMismatch
	}

	if allocation, ok := e.allocations[request.ClientID]; ok {
		if allocation.amount != request.Amount {
			return database.QuotaAcquireResult{}, database.ErrQuotaAlreadyAcquired
		}

		return e.makeResultLocked(txCtx.CurrTime, true, allocation.amount, allocation.expiresAt), nil
	}

	remaining := e.limit - e.used
	if remaining < request.Amount {
		return e.makeResultLocked(txCtx.CurrTime, false, 0, 0), nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return database.QuotaAcquireResult{}, fmt.Errorf("before apply quota acquire: %w", err)
		}
	}

	e.allocations[request.ClientID] = quotaAllocation{
		amount:    request.Amount,
		expiresAt: request.ExpiresAt,
		txAt:      txCtx.CurrTime,
		tx:        txCtx.Tx,
	}
	e.used += request.Amount
	e.ver = txCtx.Tx

	result := e.makeResultLocked(txCtx.CurrTime, true, request.Amount, request.ExpiresAt)
	result.Mutated = true

	return result, nil
}

func (e *QuotaElem) Release(
	txCtx database.TxContext,
	clientID string,
	beforeApply func() error,
) (database.QuotaReleaseResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(txCtx.CurrTime)
	allocation, ok := e.allocations[clientID]
	if !ok {
		return database.QuotaReleaseResult{}, nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return database.QuotaReleaseResult{}, fmt.Errorf("before apply quota release: %w", err)
		}
	}

	delete(e.allocations, clientID)
	e.used -= allocation.amount
	if e.used < 0 {
		e.used = 0
	}
	e.ver = txCtx.Tx

	return database.QuotaReleaseResult{
		Released:  true,
		Amount:    allocation.amount,
		Used:      e.used,
		Remaining: e.remainingLocked(),
		ExpiresAt: allocation.expiresAt,
	}, nil
}

func (e *QuotaElem) CanDelete(now database.TxTime) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(now)

	return len(e.allocations) == 0
}

func (e *QuotaElem) Info(now database.TxTime) database.QuotaInfo {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(now)

	clients := make([]database.QuotaClientInfo, 0, len(e.allocations))
	for clientID, allocation := range e.allocations {
		clients = append(clients, database.QuotaClientInfo{
			ClientID:  clientID,
			Amount:    allocation.amount,
			ExpiresAt: allocation.expiresAt,
		})
	}
	sort.Slice(clients, func(i, j int) bool {
		return clients[i].ClientID < clients[j].ClientID
	})

	return database.QuotaInfo{
		Limit:     e.limit,
		Used:      e.used,
		Remaining: e.remainingLocked(),
		Clients:   clients,
	}
}

func (e *QuotaElem) Clean(now database.TxTime) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(now)

	return len(e.allocations) == 0
}

//nolint:revive // ok
func (e *QuotaElem) DumpAllocations(dumpTx database.Tx, now database.TxTime) []quotaDumpAllocation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	res := make([]quotaDumpAllocation, 0, len(e.allocations))
	for clientID, allocation := range e.allocations {
		if allocation.tx > dumpTx {
			continue
		}
		if allocation.expiresAt != 0 && allocation.expiresAt <= now {
			continue
		}

		res = append(res, quotaDumpAllocation{
			clientID:        clientID,
			quotaAllocation: allocation,
		})
	}

	return res
}

func (e *QuotaElem) RestoreAllocation(elem database.DumpElem) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ownership := elem.Ownership
	if ownership == database.QuotaOwnershipUnknown {
		ownership = database.QuotaOwnershipClientLease
	}
	if e.limit == 0 {
		e.limit = elem.Limit
	}
	if e.ownership == database.QuotaOwnershipUnknown {
		e.ownership = ownership
	}
	if e.allocations == nil {
		e.allocations = make(map[string]quotaAllocation)
	}

	if existing, ok := e.allocations[elem.ClientID]; ok {
		e.used -= existing.amount
	}

	e.allocations[elem.ClientID] = quotaAllocation{
		amount:    elem.Value,
		expiresAt: elem.ExpiresAt,
		txAt:      elem.TxAt,
		tx:        elem.Tx,
	}
	e.used += elem.Value
	e.ver = elem.Tx
}

func (e *QuotaElem) RestoreConfig(elem database.DumpElem) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.limit = elem.Limit
	e.ownership = elem.Ownership
	e.ver = elem.Tx
	if e.allocations == nil {
		e.allocations = make(map[string]quotaAllocation)
	}
}

func (e *QuotaElem) Limit() database.ValueType {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.limit
}

func (e *QuotaElem) Ownership() database.QuotaOwnership {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.ownership
}

func (e *QuotaElem) DumpConfig(
	dumpTx database.Tx,
) (database.ValueType, database.QuotaOwnership, database.Tx, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ownership != database.QuotaOwnershipServer || e.ver == database.NoTx || e.ver > dumpTx {
		return 0, database.QuotaOwnershipUnknown, database.NoTx, false
	}

	return e.limit, e.ownership, e.ver, true
}

func (e *QuotaElem) cleanExpiredLocked(now database.TxTime) {
	for clientID, allocation := range e.allocations {
		if allocation.expiresAt == 0 || allocation.expiresAt > now {
			continue
		}

		delete(e.allocations, clientID)
		e.used -= allocation.amount
	}
	if e.used < 0 {
		e.used = 0
	}
}

func (e *QuotaElem) makeResultLocked(
	now database.TxTime,
	acquired bool,
	allocated database.ValueType,
	expiresAt database.TxTime,
) database.QuotaAcquireResult {
	expiresAfter := uint32(0)
	if expiresAt > now {
		expiresAfter = uint32(expiresAt - now)
	}

	return database.QuotaAcquireResult{
		Acquired:     acquired,
		Allocated:    allocated,
		Used:         e.used,
		Remaining:    e.remainingLocked(),
		ExpiresAfter: expiresAfter,
	}
}

func (e *QuotaElem) remainingLocked() database.ValueType {
	remaining := e.limit - e.used
	if remaining < 0 {
		return 0
	}

	return remaining
}
