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

type quotaDumpConfig struct {
	limit     database.ValueType
	ownership database.QuotaOwnership
	policy    database.QuotaPolicy
	clients   uint32
	tx        database.Tx
	ok        bool
}

type QuotaElem struct {
	limit       database.ValueType
	ownership   database.QuotaOwnership
	policy      database.QuotaPolicy
	clients     uint32
	configTx    database.Tx
	used        database.ValueType
	allocations map[string]quotaAllocation
	ver         database.Tx
	mu          sync.RWMutex
}

func NewQuotaElem(limit database.ValueType, ownership database.QuotaOwnership) *QuotaElem {
	return &QuotaElem{
		limit:       limit,
		ownership:   ownership,
		policy:      database.QuotaPolicyUnknown,
		allocations: make(map[string]quotaAllocation),
		ver:         database.NoTx,
	}
}

func (e *QuotaElem) SetConfig(
	txCtx database.TxContext,
	request database.QuotaSetRequest,
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
	if request.Policy == database.QuotaPolicyUnknown {
		request.Policy = database.QuotaPolicyFixed
	}
	if e.policy == database.QuotaPolicyUnknown {
		e.policy = request.Policy
	}
	if e.policy != database.QuotaPolicyUnknown && e.policy != request.Policy {
		return false, database.ErrQuotaPolicyMismatch
	}
	if e.used > request.Limit {
		return false, database.ErrQuotaLimitBelowUsed
	}
	if e.limit == request.Limit && e.policy == request.Policy && e.clients == request.Clients {
		return false, nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return false, fmt.Errorf("before apply quota set: %w", err)
		}
	}

	e.limit = request.Limit
	e.ownership = database.QuotaOwnershipServer
	e.policy = request.Policy
	e.clients = request.Clients
	e.configTx = txCtx.Tx
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
	if request.Policy == database.QuotaPolicyUnknown {
		request.Policy = database.QuotaPolicyFixed
	}
	if e.policy == database.QuotaPolicyUnknown {
		e.policy = request.Policy
	}
	if e.policy != request.Policy {
		return database.QuotaAcquireResult{}, database.ErrQuotaPolicyMismatch
	}
	if e.limit != request.Limit {
		return database.QuotaAcquireResult{}, database.ErrQuotaLimitMismatch
	}

	if allocation, ok := e.allocations[request.ClientID]; ok {
		if request.Policy == database.QuotaPolicyPerClient &&
			(request.Amount == 0 || allocation.amount <= request.Amount) {
			return e.makeResultLocked(txCtx.CurrTime, true, allocation.amount, allocation.expiresAt), nil
		}
		if allocation.amount != request.Amount {
			return database.QuotaAcquireResult{}, database.ErrQuotaAlreadyAcquired
		}

		return e.makeResultLocked(txCtx.CurrTime, true, allocation.amount, allocation.expiresAt), nil
	}

	allocated := e.allocateAmountLocked(request.Amount)
	if allocated <= 0 {
		return e.makeResultLocked(txCtx.CurrTime, false, 0, 0), nil
	}
	if allocated < request.Amount && request.Policy == database.QuotaPolicyFixed {
		return e.makeResultLocked(txCtx.CurrTime, false, 0, 0), nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return database.QuotaAcquireResult{}, fmt.Errorf("before apply quota acquire: %w", err)
		}
	}

	e.allocations[request.ClientID] = quotaAllocation{
		amount:    allocated,
		expiresAt: request.ExpiresAt,
		txAt:      txCtx.CurrTime,
		tx:        txCtx.Tx,
	}
	e.used += allocated
	e.ver = txCtx.Tx

	result := e.makeResultLocked(txCtx.CurrTime, true, allocated, request.ExpiresAt)
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

func (e *QuotaElem) AllocationCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.allocations)
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

func (e *QuotaElem) dumpAllocations(dumpTx database.Tx, now database.TxTime) []quotaDumpAllocation {
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
	if elem.Policy != database.QuotaPolicyUnknown {
		e.policy = elem.Policy
	} else if e.policy == database.QuotaPolicyUnknown {
		e.policy = database.QuotaPolicyFixed
	}
	if elem.Clients > 0 {
		e.clients = elem.Clients
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
	e.policy = elem.Policy
	e.clients = elem.Clients
	e.configTx = elem.Tx
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

func (e *QuotaElem) Policy() database.QuotaPolicy {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.policy
}

func (e *QuotaElem) Clients() uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.clients
}

func (e *QuotaElem) dumpConfig(dumpTx database.Tx) quotaDumpConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ownership != database.QuotaOwnershipServer || e.configTx == database.NoTx || e.configTx > dumpTx {
		return quotaDumpConfig{}
	}

	return quotaDumpConfig{
		limit:     e.limit,
		ownership: e.ownership,
		policy:    e.policy,
		clients:   e.clients,
		tx:        e.configTx,
		ok:        true,
	}
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

func (e *QuotaElem) allocateAmountLocked(amount database.ValueType) database.ValueType {
	remaining := e.remainingLocked()
	if e.policy != database.QuotaPolicyPerClient {
		if remaining < amount {
			return 0
		}

		return amount
	}

	if e.clients == 0 {
		return 0
	}
	perClient := e.limit / database.ValueType(e.clients)
	if perClient <= 0 {
		return 0
	}
	if amount > 0 && amount < perClient {
		perClient = amount
	}
	if remaining < perClient {
		return remaining
	}

	return perClient
}
