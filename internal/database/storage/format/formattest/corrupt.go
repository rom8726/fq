package formattest

import (
	"encoding/binary"
	"testing"

	"github.com/fq-db/fq/internal/database/storage/format"
)

func CorruptByte(t *testing.T, data []byte, offset int) []byte {
	t.Helper()

	if offset < 0 || offset >= len(data) {
		t.Fatalf("corrupt offset %d is out of range for %d bytes", offset, len(data))
	}

	result := append([]byte(nil), data...)
	result[offset] ^= 0xff

	return result
}

func CorruptLength(t *testing.T, data []byte, frameOffset int) []byte {
	t.Helper()

	return CorruptByte(t, data, frameOffset+3)
}

func CorruptChecksum(t *testing.T, data []byte, frameOffset int) []byte {
	t.Helper()

	return CorruptByte(t, data, frameOffset+4)
}

func CorruptPayload(t *testing.T, data []byte, frameOffset int) []byte {
	t.Helper()

	return CorruptByte(t, data, frameOffset+format.FrameHeaderSize)
}

func CorruptMagic(t *testing.T, data []byte) []byte {
	t.Helper()

	return CorruptByte(t, data, 0)
}

func SetVersion(t *testing.T, data []byte, version uint16) []byte {
	t.Helper()

	if len(data) < format.HeaderSize {
		t.Fatalf("data is shorter than format header: %d bytes", len(data))
	}

	result := append([]byte(nil), data...)
	binary.BigEndian.PutUint16(result[4:6], version)

	return result
}
