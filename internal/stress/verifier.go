package stress

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
	response, err := v.Query(ctx, query)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(response, "ok|") {
		return fmt.Errorf("query %q response = %q, want ok| prefix", query, response)
	}

	return nil
}

func (v *Verifier) ExpectValue(ctx context.Context, key string, window, want uint64) error {
	response, err := v.Query(ctx, fmt.Sprintf("GET %s %d", key, window))
	if err != nil {
		return err
	}

	wantResponse := "ok|" + strconv.FormatUint(want, 10)
	if response != wantResponse {
		return fmt.Errorf("GET %s %d response = %q, want %q", key, window, response, wantResponse)
	}

	return nil
}
