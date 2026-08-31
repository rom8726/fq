package database

import (
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/security"
)

type stubStorage struct{}

func (stubStorage) Incr(context.Context, BatchKey) (ValueType, error) { return 1, nil }
func (stubStorage) Get(context.Context, BatchKey) (ValueType, error)  { return 1, nil }
func (stubStorage) Del(context.Context, BatchKey) (bool, error)       { return true, nil }
func (stubStorage) MDel(context.Context, []BatchKey) ([]bool, error)  { return []bool{true}, nil }
func (stubStorage) Watch(context.Context, BatchKey) (ValueType, error) {
	return 1, nil
}

func (stubStorage) SubscribeLimitEvents(context.Context, string) (<-chan LimitEvent, func()) {
	channel := make(chan LimitEvent)

	return channel, func() { close(channel) }
}

func (stubStorage) SubscribeQuotaEvents(context.Context, string) (<-chan QuotaEvent, func()) {
	channel := make(chan QuotaEvent)

	return channel, func() { close(channel) }
}

func (stubStorage) RLimitFixedWindow(context.Context, BatchKey, ValueType) (RateLimitResult, error) {
	return RateLimitResult{Allowed: true}, nil
}

func (stubStorage) RLimitSlidingWindow(context.Context, BatchKey, ValueType) (RateLimitResult, error) {
	return RateLimitResult{Allowed: true}, nil
}

func (stubStorage) RLimitTokenBucket(
	context.Context,
	BatchKey,
	ValueType,
	ValueType,
) (RateLimitResult, error) {
	return RateLimitResult{Allowed: true}, nil
}

func (stubStorage) QuotaAcquire(context.Context, QuotaAcquireRequest) (QuotaAcquireResult, error) {
	return QuotaAcquireResult{Acquired: true}, nil
}

func (stubStorage) QuotaSet(context.Context, QuotaSetRequest) (bool, error)    { return true, nil }
func (stubStorage) QuotaRelease(context.Context, string, string) (bool, error) { return true, nil }
func (stubStorage) QuotaDelete(context.Context, string) (bool, error)          { return true, nil }
func (stubStorage) QuotaInfo(context.Context, string) (QuotaInfo, error) {
	return QuotaInfo{}, nil
}
func (stubStorage) FlushDB(context.Context) error  { return nil }
func (stubStorage) Truncate(context.Context) error { return nil }
func (stubStorage) Scan(context.Context, string, string, uint32) (ScanResult, error) {
	return ScanResult{}, nil
}

func newTestDatabase(t *testing.T) *Database {
	t.Helper()

	logger := zerolog.Nop()
	comp := compute.NewCompute(compute.NewParser(&logger), compute.NewAnalyzer(&logger), &logger)

	return NewDatabase(comp, stubStorage{}, &logger, 4096)
}

func authContext(t *testing.T) (context.Context, *security.Session) {
	t.Helper()

	registry := security.NewRegistry()
	require.NoError(t, registry.Add("admin-token-value", security.RoleAdmin))
	require.NoError(t, registry.Add("rw-token-value", security.RoleRW))
	require.NoError(t, registry.Add("ro-token-value", security.RoleRO))

	session := security.NewSession(registry)

	return security.WithSession(context.Background(), session), session
}

func TestUnauthenticatedCommandsAreRejected(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := authContext(t)

	for _, query := range []string{"GET key 60", "INCR key 60", "FLUSHDB", "TRUNCATE", "INSPECT"} {
		response := db.HandleQuery(ctx, query)
		require.True(t, strings.HasPrefix(response, "err|"), query)
		require.Contains(t, response, "not authenticated", query)
	}
}

func TestHelloNeedsNoAuthentication(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := authContext(t)

	require.Equal(t, "ok|1;4096;1;none", db.HandleQuery(ctx, "HELLO 1"))
	require.False(t, requiresAuthorization(compute.HelloCommandID))
	require.False(t, requiresAuthorization(compute.AuthCommandID))
	require.True(t, requiresAuthorization(compute.GetCommandID))
}

func TestAuthGrantsRoleAndRoleGatesCommands(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := authContext(t)

	require.Equal(t, "ok|1", db.HandleQuery(ctx, "AUTH ro-token-value"))
	require.Contains(t, db.HandleQuery(ctx, "SCAN cursor 10"), "ok|")
	require.Contains(t, db.HandleQuery(ctx, "INSPECT"), "permission denied")
	require.Contains(t, db.HandleQuery(ctx, "INCR key 60"), "permission denied")
	require.Contains(t, db.HandleQuery(ctx, "DEL key 60"), "permission denied")
	require.Contains(t, db.HandleQuery(ctx, "FLUSHDB"), "permission denied")

	require.Equal(t, "ok|1", db.HandleQuery(ctx, "AUTH rw-token-value"))
	require.NotContains(t, db.HandleQuery(ctx, "INCR key 60"), "denied")
	require.NotContains(t, db.HandleQuery(ctx, "RLIMIT FW key 100 60"), "denied")
	require.Contains(t, db.HandleQuery(ctx, "TRUNCATE"), "permission denied")
	require.Contains(t, db.HandleQuery(ctx, "FLUSHDB"), "permission denied")

	require.Equal(t, "ok|1", db.HandleQuery(ctx, "AUTH admin-token-value"))
	require.NotContains(t, db.HandleQuery(ctx, "INSPECT"), "denied")
	require.NotContains(t, db.HandleQuery(ctx, "FLUSHDB"), "denied")
	require.NotContains(t, db.HandleQuery(ctx, "TRUNCATE"), "denied")
}

func TestQuotaInfNeedsOnlyReadRole(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := authContext(t)

	require.Equal(t, "ok|1", db.HandleQuery(ctx, "AUTH ro-token-value"))
	require.NotContains(t, db.HandleQuery(ctx, "QUOTA INF some-quota"), "permission denied")
	require.Contains(t, db.HandleQuery(ctx, "QUOTA DEL some-quota"), "permission denied")
}

func TestBadAuthIsReportedAndLimited(t *testing.T) {
	db := newTestDatabase(t)
	ctx, session := authContext(t)

	for i := 1; i < security.MaxAuthFailures; i++ {
		require.Contains(t, db.HandleQuery(ctx, "AUTH wrong-token-value"), "authentication failed")
	}

	err := db.HandleQueryStream(ctx, "AUTH wrong-token-value", func([]byte) error { return nil })
	require.ErrorIs(t, err, security.ErrTooManyAuthFailures)
	require.Equal(t, security.MaxAuthFailures, session.Failures())
}

func TestNoSessionMeansNoEnforcement(t *testing.T) {
	db := newTestDatabase(t)

	require.NotContains(t, db.HandleQuery(context.Background(), "FLUSHDB"), "not authenticated")
}

func TestRedactQuery(t *testing.T) {
	require.Equal(t, "AUTH [REDACTED]", redactQuery("AUTH super-secret"))
	require.Equal(t, "AUTH [REDACTED]", redactQuery("  auth super-secret"))
	require.Equal(t, "AUTH [REDACTED]", redactQuery("AUTH"))
	require.Equal(t, "GET key 60", redactQuery("GET key 60"))
	require.Equal(t, "AUTHENTICATE x", redactQuery("AUTHENTICATE x"))
}

func TestCommandRoleMatrix(t *testing.T) {
	tests := map[compute.CommandID]security.Role{
		compute.GetCommandID:      security.RoleRO,
		compute.ScanCommandID:     security.RoleRO,
		compute.InspectCommandID:  security.RoleAdmin,
		compute.IncrCommandID:     security.RoleRW,
		compute.MDelCommandID:     security.RoleRW,
		compute.FlushDBCommandID:  security.RoleAdmin,
		compute.TruncateCommandID: security.RoleAdmin,
	}

	for commandID, want := range tests {
		require.Equal(t, want, commandRole(compute.NewQuery(commandID, nil)), commandID.Int())
	}

	require.Equal(t, security.RoleRO, commandRole(compute.NewQuery(compute.QuotaCommandID, []string{"INF", "q"})))
	require.Equal(t, security.RoleRW, commandRole(compute.NewQuery(compute.QuotaCommandID, []string{"DEL", "q"})))
	require.Equal(t, security.RoleAdmin, commandRole(compute.NewQuery(compute.UnknownCommandID, nil)))
}
