package database

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponseFormatting(t *testing.T) {
	t.Parallel()

	require.Equal(t, "err|boom", string(makeErrorMsg(errors.New("boom"))))
	require.Equal(t, "ok|42", string(appendValueMsg(nil, 42)))
	require.Equal(t, "ok|1", string(makeBoolMsg(true)))
	require.Equal(t, "ok|0", string(makeBoolMsg(false)))
	require.Equal(t, "ok|1;0;1", string(appendBoolsMsg(nil, []bool{true, false, true})))
	require.Equal(t, "ok|1;7;3;60", string(appendRateLimitMsg(nil, RateLimitResult{
		Allowed:    true,
		Current:    7,
		Remaining:  3,
		ResetAfter: 60,
	})))
	require.Equal(t, "ok|tenant-a;60;100;5", string(appendLimitEventMsg(nil, LimitEvent{
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
			_ = appendValueMsg(nil, ValueType(i))
		}
	})

	b.Run("value_append", func(b *testing.B) {
		buf := make([]byte, 0, len("ok|18446744073709551615"))

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf = appendValueMsg(buf[:0], ValueType(i))
		}
	})

	b.Run("bools", func(b *testing.B) {
		values := []bool{true, false, true, true}

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = appendBoolsMsg(nil, values)
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
			_ = appendRateLimitMsg(nil, result)
		}
	})
}
