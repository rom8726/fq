package network

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func FuzzReadFrameSize(f *testing.F) {
	seeds := [][]byte{
		{0, 0, 0, 0},
		{0, 0, 0, 1},
		{0xff, 0xff, 0xff, 0xff},
		{0, 0, 0},
		{},
	}
	for _, seed := range seeds {
		f.Add(seed, 1<<20)
	}

	f.Fuzz(func(t *testing.T, data []byte, maxMessageSize int) {
		if maxMessageSize < 0 {
			maxMessageSize = -maxMessageSize
		}

		var header [frameHeaderSize]byte
		reader := bytes.NewReader(data)

		size, err := readFrameSize(reader, header[:], maxMessageSize)
		if err != nil {
			return
		}

		if size < 0 || size > maxMessageSize {
			t.Fatalf("readFrameSize returned out-of-range size %d for max %d", size, maxMessageSize)
		}

		wantSize := int(binary.BigEndian.Uint32(data[:frameHeaderSize]))
		if size != wantSize {
			t.Fatalf("readFrameSize returned %d, want %d", size, wantSize)
		}
	})
}

func FuzzReadFrameInto(f *testing.F) {
	seeds := [][]byte{
		append([]byte{0, 0, 0, 3}, []byte("abc")...),
		{0, 0, 0, 0},
		{0, 0, 0, 5, 1, 2},
		{},
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bytes.NewReader(data)
		buffer := make([]byte, 1<<16)

		message, err := readFrameInto(reader, 1<<20, buffer)
		if err != nil {
			return
		}

		if len(message) > len(data) {
			t.Fatalf("readFrameInto returned message longer than input: %d > %d", len(message), len(data))
		}
	})
}
