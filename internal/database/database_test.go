package database

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseFormatting(t *testing.T) {
	t.Parallel()

	require.Equal(t, "err|boom", string(makeErrorMsg(errors.New("boom"))))
	require.Equal(t, "ok|42", string(makeValueMsg(42)))
	require.Equal(t, "ok|1", string(makeBoolMsg(true)))
	require.Equal(t, "ok|0", string(makeBoolMsg(false)))
	require.Equal(t, "ok|1;0;1", string(makeBoolsMsg([]bool{true, false, true})))
	require.Equal(t, "ok|1;7;3;60", string(makeRateLimitMsg(RateLimitResult{
		Allowed:    true,
		Current:    7,
		Remaining:  3,
		ResetAfter: 60,
	})))
	require.Equal(t, "ok|tenant-a;60;100;5", string(makeLimitEventMsg(LimitEvent{
		Key:        "tenant-a",
		Window:     60,
		Current:    100,
		ResetAfter: 5,
	})))
}

func BenchmarkResponseFormatting(b *testing.B) {
	b.Run("value", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = makeValueMsg(ValueType(i))
		}
	})

	b.Run("bools", func(b *testing.B) {
		values := []bool{true, false, true, true}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = makeBoolsMsg(values)
		}
	})

	b.Run("rate_limit", func(b *testing.B) {
		result := RateLimitResult{
			Allowed:    true,
			Current:    123,
			Remaining:  456,
			ResetAfter: 789,
		}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = makeRateLimitMsg(result)
		}
	})
}
