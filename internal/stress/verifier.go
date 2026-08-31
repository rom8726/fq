package stress

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/fq-db/fq/internal/network"
)

type Verifier struct {
	address        string
	maxMessageSize int
	idleTimeout    time.Duration
}

func NewVerifier(address string, maxMessageSize int, idleTimeout time.Duration) *Verifier {
	return &Verifier{
		address:        address,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
	}
}

func (v *Verifier) Query(ctx context.Context, query string) (string, error) {
	client, err := network.NewTCPClient(v.address, v.maxMessageSize, v.idleTimeout)
	if err != nil {
		return "", err
	}
	defer func() { _ = client.Close() }()

	response, err := client.Send(ctx, []byte(query))
	if err != nil {
		return "", err
	}

	return string(response), nil
}

func (v *Verifier) ExpectOK(ctx context.Context, query string) error {
	if _, err := v.Query(ctx, query); err != nil {
		return fmt.Errorf("query %q failed: %w", query, err)
	}

	return nil
}

func (v *Verifier) ExpectValue(ctx context.Context, key string, window, want uint64) error {
	response, err := v.Query(ctx, fmt.Sprintf("GET %s %d", key, window))
	if err != nil {
		return err
	}

	wantResponse := strconv.FormatUint(want, 10)
	if response != wantResponse {
		return fmt.Errorf("GET %s %d response = %q, want %q", key, window, response, wantResponse)
	}

	return nil
}

func (v *Verifier) ExpectValueAtLeast(ctx context.Context, key string, window, minValue uint64) error {
	response, err := v.Query(ctx, fmt.Sprintf("GET %s %d", key, window))
	if err != nil {
		return err
	}

	value, ok := parseOKUint(response)
	if !ok {
		return fmt.Errorf("GET %s %d response = %q, want a uint64", key, window, response)
	}
	if value < minValue {
		return fmt.Errorf("GET %s %d response = %q, want at least %d", key, window, response, minValue)
	}

	return nil
}
