package wal

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/fq-db/fq/internal/database/storage/format"
)

const (
	MaxBatchSize = 100 * 1024 * 1024

	segmentFormatVersionRaw        uint16 = 1
	segmentFormatVersionCompressed uint16 = 2

	metadataFormatVersion = 1
	metadataMaxFrameSize  = 8
	lsnPayloadSize        = 8
)

func segmentHeader(version uint16) []byte {
	return format.AppendHeader(nil, format.MagicWAL, version)
}

func SegmentFormatVersion(path string) (uint16, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()

	head := make([]byte, format.HeaderSize)
	if _, err := file.ReadAt(head, 0); err != nil {
		return 0, fmt.Errorf("read WAL segment header: %w", err)
	}

	_, version, err := format.ParseHeaderVersions(
		head,
		format.MagicWAL,
		segmentFormatVersionRaw,
		segmentFormatVersionCompressed,
	)
	if err != nil {
		return 0, err
	}

	return version, nil
}

func encodeLSNFile(lsn uint64) []byte {
	payload := make([]byte, lsnPayloadSize)
	binary.BigEndian.PutUint64(payload, lsn)

	return format.AppendFrame(format.AppendHeader(nil, format.MagicMeta, metadataFormatVersion), payload)
}

func decodeLSNFile(data []byte) (uint64, error) {
	rest, err := format.ParseHeader(data, format.MagicMeta, metadataFormatVersion)
	if err != nil {
		return 0, err
	}

	payload, trailing, err := format.NextFrame(rest, metadataMaxFrameSize)
	if err != nil {
		return 0, err
	}

	if len(payload) != lsnPayloadSize {
		return 0, fmt.Errorf("unexpected LSN payload size: %d", len(payload))
	}

	if len(trailing) != 0 {
		return 0, fmt.Errorf("unexpected trailing data after LSN frame: %d bytes", len(trailing))
	}

	return binary.BigEndian.Uint64(payload), nil
}
