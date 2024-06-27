package replication

import (
	"context"
	"fmt"
	"os"
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
		err = s.handleResponse(response)
		if err != nil {
			return fmt.Errorf("handle wal response: %w", err)
		}

		return nil
	}

	return fmt.Errorf("failed to apply replication data: master error")
}

func (s *Slave) handleResponse(response WALResponse) error {
	if response.SegmentName == "" {
		s.logger.Debug().Msg("no changes from replication")

		return nil
	}

	filename := response.SegmentName

	if err := s.saveWALSegment(filename, response.SegmentData); err != nil {
		return fmt.Errorf("save wal segment: %w", err)
	}

	if err := s.applyDataToEngine(response.SegmentData); err != nil {
		return fmt.Errorf("apply data to engine segment: %w", err)
	}

	s.lastSegmentName = response.SegmentName

	return nil
}

func (s *Slave) saveWALSegment(segmentName string, segmentData []byte) error {
	flags := os.O_CREATE | os.O_WRONLY
	filename := fmt.Sprintf("%s/%s", s.walDirectory, segmentName)
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
func (s *Slave) applyDataToEngine(segmentData []byte) error {
	// var logs []wal.LogData
	//buffer := bytes.NewBuffer(segmentData)
	//decoder := gob.NewDecoder(buffer)
	//if err := decoder.Decode(&logs); err != nil {
	//	return fmt.Errorf("failed to decode data: %w", err)
	//}
	//
	//s.walStream <- logs

	return nil
}
