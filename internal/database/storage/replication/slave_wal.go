package replication

import (
	"context"
	"fmt"
	"os"
)

func (s *Slave) synchronizeWAL() {
	request := NewWALRequest(s.lastSegmentName)

	requestData, err := Encode(&request)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode replication request")
	}

	responseData, err := s.client.Send(context.TODO(), requestData)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to send replication request")
	}

	var response WALResponse
	if err = Decode(&response, responseData); err != nil {
		s.logger.Error().Err(err).Msg("failed to decode replication response")
	}

	if response.Succeed {
		s.handleResponse(response)
	} else {
		s.logger.Error().Err(err).Msg("failed to apply replication data: master error")
	}
}

func (s *Slave) handleResponse(response WALResponse) {
	if response.SegmentName == "" {
		s.logger.Debug().Msg("no changes from replication")

		return
	}

	filename := response.SegmentName

	if err := s.saveWALSegment(filename, response.SegmentData); err != nil {
		s.logger.Error().Err(err).Msg("failed to apply replication data")
	}

	if err := s.applyDataToEngine(response.SegmentData); err != nil {
		s.logger.Error().Err(err).Msg("failed to apply replication data")
	}

	s.lastSegmentName = response.SegmentName
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
	//s.stream <- logs

	return nil
}
