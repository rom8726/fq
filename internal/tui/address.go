package tui

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/fq-db/fq/internal/dbcli"
	"github.com/fq-db/fq/internal/network"
)

func dialAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}

	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	return net.JoinHostPort(host, port)
}

func dialWithRetry(
	ctx context.Context,
	options dbcli.ConnectOptions,
	maxWait time.Duration,
	retryInterval time.Duration,
) (*network.TCPClient, error) {
	deadline := time.Now().Add(maxWait)
	var lastErr error

	for {
		client, err := dbcli.Connect(ctx, options)
		if err == nil {
			return client, nil
		}
		lastErr = err

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect to %s: %w", options.Address, lastErr)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}
