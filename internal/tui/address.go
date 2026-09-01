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
	retryInterval time.Duration,
	notifyInterval time.Duration,
	notify func(err error),
) (*network.TCPClient, error) {
	lastNotify := time.Now()

	for {
		client, err := dbcli.Connect(ctx, options)
		if err == nil {
			return client, nil
		}

		if notify != nil && time.Since(lastNotify) >= notifyInterval {
			notify(err)
			lastNotify = time.Now()
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connect to %s: %w", options.Address, ctx.Err())
		case <-time.After(retryInterval):
		}
	}
}
