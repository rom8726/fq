package replication

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"fq/internal/database"
)

type Request struct {
	DumpRequest
	WALRequest
}

type DumpRequest struct {
	SessionUUID       string
	LastSegmentNumber uint64
}

type DumpResponse struct {
	Succeed     bool
	EndOfDump   bool
	SegmentData []database.DumpElem
}

type WALRequest struct {
	LastSegmentName string
}

type WALResponse struct {
	Succeed     bool
	SegmentName string
	SegmentData []byte
}

func NewDumpRequest(sessionUUID string, lastSegmentNumber uint64) Request {
	return Request{
		DumpRequest: DumpRequest{
			SessionUUID:       sessionUUID,
			LastSegmentNumber: lastSegmentNumber,
		},
	}
}

func NewWALRequest(lastSegmentName string) Request {
	return Request{
		WALRequest: WALRequest{LastSegmentName: lastSegmentName},
	}
}

func Encode[ProtocolObject Request | WALResponse | DumpResponse](object *ProtocolObject) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(object); err != nil {
		return nil, fmt.Errorf("failed to encode object: %w", err)
	}

	return buffer.Bytes(), nil
}

func Decode[ProtocolObject Request | WALResponse | DumpResponse](object *ProtocolObject, data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&object); err != nil {
		return fmt.Errorf("failed to decode object: %w", err)
	}

	return nil
}
