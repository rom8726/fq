package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

type Magic [4]byte

var (
	MagicWAL  = Magic{'F', 'Q', 'W', 'L'}
	MagicDump = Magic{'F', 'Q', 'D', 'P'}
	MagicMeta = Magic{'F', 'Q', 'M', 'T'}
)

const (
	HeaderSize      = 8
	FrameHeaderSize = 8

	magicSize   = 4
	versionSize = 2
	lengthSize  = 4
)

var (
	ErrBadMagic           = errors.New("unsupported format magic")
	ErrUnsupportedVersion = errors.New("unsupported format version")
	ErrChecksumMismatch   = errors.New("checksum mismatch")
	ErrIncompleteFrame    = errors.New("incomplete frame")
	ErrFrameTooLarge      = errors.New("frame too large")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func (m Magic) String() string {
	return string(m[:])
}

func AppendHeader(dst []byte, magic Magic, version uint16) []byte {
	header := make([]byte, HeaderSize)
	copy(header, magic[:])
	binary.BigEndian.PutUint16(header[magicSize:magicSize+versionSize], version)

	return append(dst, header...)
}

func WriteHeader(w io.Writer, magic Magic, version uint16) error {
	if _, err := w.Write(AppendHeader(nil, magic, version)); err != nil {
		return fmt.Errorf("write format header: %w", err)
	}

	return nil
}

func ParseHeader(data []byte, magic Magic, version uint16) ([]byte, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("%w: format header needs %d bytes, got %d", ErrIncompleteFrame, HeaderSize, len(data))
	}

	var got Magic
	copy(got[:], data[:magicSize])
	if got != magic {
		return nil, fmt.Errorf("%w: want %q, got %q", ErrBadMagic, magic.String(), got.String())
	}

	gotVersion := binary.BigEndian.Uint16(data[magicSize : magicSize+versionSize])
	if gotVersion != version {
		return nil, fmt.Errorf("%w: want %d, got %d", ErrUnsupportedVersion, version, gotVersion)
	}

	return data[HeaderSize:], nil
}

func FrameHeader(payload []byte) []byte {
	head := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(head[:lengthSize], uint32(len(payload)))
	binary.BigEndian.PutUint32(head[lengthSize:], checksum(head[:lengthSize], payload))

	return head
}

func AppendFrame(dst, payload []byte) []byte {
	dst = append(dst, FrameHeader(payload)...)

	return append(dst, payload...)
}

func NextFrame(data []byte, maxFrame int) (payload, rest []byte, err error) {
	if len(data) < FrameHeaderSize {
		return nil, data, fmt.Errorf(
			"%w: frame header needs %d bytes, got %d",
			ErrIncompleteFrame,
			FrameHeaderSize,
			len(data),
		)
	}

	length := int(binary.BigEndian.Uint32(data[:lengthSize]))
	if maxFrame > 0 && length > maxFrame {
		return nil, data, fmt.Errorf("%w: %d bytes (max %d)", ErrFrameTooLarge, length, maxFrame)
	}

	if len(data)-FrameHeaderSize < length {
		return nil, data, fmt.Errorf(
			"%w: declared %d bytes, got %d",
			ErrIncompleteFrame,
			length,
			len(data)-FrameHeaderSize,
		)
	}

	payload = data[FrameHeaderSize : FrameHeaderSize+length]

	want := binary.BigEndian.Uint32(data[lengthSize:FrameHeaderSize])
	got := checksum(data[:lengthSize], payload)
	if want != got {
		return nil, data, fmt.Errorf("%w: want %08x, got %08x", ErrChecksumMismatch, want, got)
	}

	return payload, data[FrameHeaderSize+length:], nil
}

func CompleteFramesSize(data []byte, hasHeader bool, maxFrame int) int {
	offset, ok := framesOffset(data, hasHeader)
	if !ok {
		return 0
	}

	complete := 0
	for {
		size, ok := frameSize(data, offset, maxFrame)
		if !ok {
			return complete
		}

		offset += size
		complete = offset
	}
}

func FirstFrameSize(data []byte, hasHeader bool, maxFrame int) int {
	offset, ok := framesOffset(data, hasHeader)
	if !ok {
		return 0
	}

	if len(data)-offset < FrameHeaderSize {
		return 0
	}

	length := int(binary.BigEndian.Uint32(data[offset : offset+lengthSize]))
	if maxFrame > 0 && length > maxFrame {
		return 0
	}

	return offset + FrameHeaderSize + length
}

func framesOffset(data []byte, hasHeader bool) (int, bool) {
	if !hasHeader {
		return 0, true
	}

	if len(data) < HeaderSize {
		return 0, false
	}

	return HeaderSize, true
}

func frameSize(data []byte, offset, maxFrame int) (int, bool) {
	if len(data)-offset < FrameHeaderSize {
		return 0, false
	}

	length := int(binary.BigEndian.Uint32(data[offset : offset+lengthSize]))
	if maxFrame > 0 && length > maxFrame {
		return 0, false
	}

	size := FrameHeaderSize + length
	if offset+size > len(data) {
		return 0, false
	}

	return size, true
}

func checksum(lengthBytes, payload []byte) uint32 {
	return crc32.Update(crc32.Checksum(lengthBytes, castagnoli), castagnoli, payload)
}
