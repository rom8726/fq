package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClient_Incr(t *testing.T) {
	c, err := New(":1945", time.Minute)
	require.NoError(t, err)

	v, err := c.Incr(context.Background(), "qwerty", 60)
	require.NoError(t, err)
	t.Log(v)
}
