package replication

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/fq-db/fq/internal/database"
)

type Request struct {
	AuthToken string
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
	ReplicaID       string
	LastSegmentName string
	SegmentOffset   int64
	LastAppliedLSN  uint64
}

type WALResponse struct {
	Succeed           bool
	SegmentName       string
	SegmentOffset     int64
	NextSegmentOffset int64
	SegmentData       []byte
}

func NewDumpRequest(authToken, sessionUUID string, lastSegmentNumber uint64) Request {
	return Request{
		AuthToken: authToken,
		DumpRequest: DumpRequest{
			SessionUUID:       sessionUUID,
			LastSegmentNumber: lastSegmentNumber,
		},
	}
}

func NewWALRequest(
	authToken, replicaID, lastSegmentName string,
	segmentOffset int64,
	lastAppliedLSN uint64,
) Request {
	return Request{
		AuthToken: authToken,
		WALRequest: WALRequest{
			ReplicaID:       replicaID,
			LastSegmentName: lastSegmentName,
			SegmentOffset:   segmentOffset,
			LastAppliedLSN:  lastAppliedLSN,
		},
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
