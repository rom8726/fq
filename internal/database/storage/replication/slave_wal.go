package replication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func (s *Slave) synchronizeWAL(ctx context.Context) error {
	request := NewWALRequest(s.lastSegmentName)

	requestData, err := Encode(&request)
	if err != nil {
		return fmt.Errorf("encode wal request: %w", err)
	}

	responseData, err := s.client.Send(ctx, requestData)
	if err != nil {
		return fmt.Errorf("send wal request: %w", err)
	}

	var response WALResponse
	if err = Decode(&response, responseData); err != nil {
		return fmt.Errorf("decode wal response: %w", err)
	}

	if response.Succeed {
		err = s.handleResponse(ctx, response)
		if err != nil {
			return fmt.Errorf("handle wal response: %w", err)
		}

		return nil
	}

	return fmt.Errorf("failed to apply replication data: master error")
}

func (s *Slave) handleResponse(ctx context.Context, response WALResponse) error {
	if response.SegmentName == "" {
		s.logger.Debug().Msg("no changes from replication")

		return nil
	}

	filename := response.SegmentName

	if err := s.saveWALSegment(filename, response.SegmentData); err != nil {
		return fmt.Errorf("save wal segment: %w", err)
	}

	if err := s.applyDataToEngine(ctx, response.SegmentData); err != nil {
		return fmt.Errorf("apply data to engine segment: %w", err)
	}

	s.lastSegmentName = response.SegmentName

	return nil
}

func (s *Slave) saveWALSegment(segmentName string, segmentData []byte) error {
	flags := os.O_CREATE | os.O_WRONLY
	filename := filepath.Join(s.walDirectory, segmentName)
	segment, err := os.OpenFile(filename, flags, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create wal segment: %w", err)
	}

	if _, err = segment.Write(segmentData); err != nil {
		return fmt.Errorf("failed to write data to segment: %w", err)
	}

	return segment.Sync()
}

//nolint:gocritic,revive // in progress
func (s *Slave) applyDataToEngine(ctx context.Context, segmentData []byte) error {
	logs, err := s.walReader.ReadSegmentData(ctx, segmentData)
	if err != nil {
		return err
	}

	sort.Slice(logs, func(i, j int) bool {
		return logs[i].LSN < logs[j].LSN
	})

	idx := len(logs)
	for i, log := range logs {
		if log.LSN <= s.dumpLastSegmentNumber {
			continue
		} else {
			idx = i

			break
		}
	}

	if idx == len(logs) {
		s.logger.Debug().Msg("skip replicated segment")

		return nil
	}

	s.walStream <- logs[idx:]

	return nil
}
