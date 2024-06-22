//nolint:dupl // it's ok
package client

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"fq/internal/database/compute"
	"fq/internal/network"
)

type Client struct {
	client *network.TCPClient
}

func New(address string, idleTimeout time.Duration) (*Client, error) {
	client, err := network.NewTCPClient(address, 4096, idleTimeout)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
	}, nil
}

func (c *Client) Close() error {
	return nil
}

func (c *Client) Incr(ctx context.Context, key string, capping uint32) (uint64, error) {
	cappingStr := strconv.FormatUint(uint64(capping), 10)

	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	buf.WriteString(compute.IncrCommand)
	buf.WriteByte(' ')
	buf.WriteString(key)
	buf.WriteByte(' ')
	buf.WriteString(cappingStr)

	resp, err := c.client.Send(ctx, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.value, nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}

func (c *Client) Get(ctx context.Context, key string, capping uint32) (uint64, error) {
	cappingStr := strconv.FormatUint(uint64(capping), 10)

	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	buf.WriteString(compute.GetCommand)
	buf.WriteByte(' ')
	buf.WriteString(key)
	buf.WriteByte(' ')
	buf.WriteString(cappingStr)

	resp, err := c.client.Send(ctx, buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		return result.value, nil
	case ResponseStatusError:
		return 0, result.err
	default:
		return 0, ErrUnknownRespStatus
	}
}

func (c *Client) Del(ctx context.Context, key string, capping uint32) (bool, error) {
	cappingStr := strconv.FormatUint(uint64(capping), 10)

	buf := bytesBufferPool.Get()
	defer bytesBufferPool.Put(buf)

	buf.WriteString(compute.DelCommand)
	buf.WriteByte(' ')
	buf.WriteString(key)
	buf.WriteByte(' ')
	buf.WriteString(cappingStr)

	resp, err := c.client.Send(ctx, buf.Bytes())
	if err != nil {
		return false, fmt.Errorf("send: %w", err)
	}

	result, err := parseResponse(resp)
	if err != nil {
		return false, fmt.Errorf("parse response: %w", err)
	}

	switch result.status {
	case ResponseStatusSuccess:
		boolResult := result.value == 1

		return boolResult, nil
	case ResponseStatusError:
		return false, result.err
	default:
		return false, ErrUnknownRespStatus
	}
}