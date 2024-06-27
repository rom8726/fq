package replication

import (
	"context"
	"fmt"
)

func (s *Slave) synchronizeDump(ctx context.Context) error {
	request := NewDumpRequest(s.sessionUUID, s.dumpLastSegmentNumber)

	requestData, err := Encode(&request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	responseData, err := s.client.Send(ctx, requestData)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}

	var response DumpResponse
	if err = Decode(&response, responseData); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if response.Succeed {
		s.readDump = !response.EndOfDump
		s.dumpStream <- response.SegmentData

		return nil
	}

	return fmt.Errorf("failed to apply replication data: master error")
}
