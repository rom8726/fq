package replication

import (
	"context"
	"fmt"
)

func (s *Slave) synchronizeDump() {
	request := NewDumpRequest(s.sessionUUID, s.dumpLastSegmentNumber)

	requestData, err := Encode(&request)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode replication request")
	}

	responseData, err := s.client.Send(context.TODO(), requestData)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to send replication request")
	}

	var response DumpResponse
	if err = Decode(&response, responseData); err != nil {
		s.logger.Error().Err(err).Msg("failed to decode replication response")
	}

	if response.Succeed {
		s.readDump = !response.EndOfDump

		fmt.Println(response)
	} else {
		s.logger.Error().Err(err).Msg("failed to apply replication data: master error")
	}
}
