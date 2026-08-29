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
	used        database.ValueType
	allocations map[string]quotaAllocation
	ver         database.Tx
	mu          sync.RWMutex
}

func NewQuotaElem(limit database.ValueType) *QuotaElem {
	return &QuotaElem{
		limit:       limit,
		allocations: make(map[string]quotaAllocation),
		ver:         database.NoTx,
	}
}

func (e *QuotaElem) Acquire(
	txCtx database.TxContext,
	request database.QuotaAcquireRequest,
	beforeApply func() error,
) (database.QuotaAcquireResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(txCtx.CurrTime)
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

	return e.makeResultLocked(txCtx.CurrTime, true, request.Amount, request.ExpiresAt), nil
}

func (e *QuotaElem) Release(txCtx database.TxContext, clientID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.cleanExpiredLocked(txCtx.CurrTime)
	allocation, ok := e.allocations[clientID]
	if !ok {
		return false
	}

	delete(e.allocations, clientID)
	e.used -= allocation.amount
	if e.used < 0 {
		e.used = 0
	}
	e.ver = txCtx.Tx

	return true
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

	remaining := e.limit - e.used
	if remaining < 0 {
		remaining = 0
	}

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
		Remaining: remaining,
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

	if e.limit == 0 {
		e.limit = elem.Limit
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

func (e *QuotaElem) Limit() database.ValueType {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.limit
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
	remaining := e.limit - e.used
	if remaining < 0 {
		remaining = 0
	}

	expiresAfter := uint32(0)
	if expiresAt > now {
		expiresAfter = uint32(expiresAt - now)
	}

	return database.QuotaAcquireResult{
		Acquired:     acquired,
		Allocated:    allocated,
		Used:         e.used,
		Remaining:    remaining,
		ExpiresAfter: expiresAfter,
	}
}
