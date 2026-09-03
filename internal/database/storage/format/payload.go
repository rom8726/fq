package format

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	PayloadVersionRaw        uint16 = 1
	PayloadVersionCompressed uint16 = 2

	codecPrefixSize = 1
)

var ErrPayloadTooLarge = errors.New("payload too large")

func PayloadCodec(payload []byte) CodecID {
	if len(payload) < codecPrefixSize {
		return CodecNone
	}

	return CodecID(payload[0])
}

func EncodePayload(dst, raw []byte, compression Compression) []byte {
	dst = dst[:0]

	impl, ok := codecs[compression.Codec]
	if !ok || len(raw) < compression.MinFrameSize {
		return appendRawPayload(dst, raw)
	}

	compressed := impl.compress(nil, raw)

	var head [binary.MaxVarintLen64]byte
	headSize := binary.PutUvarint(head[:], uint64(len(raw)))

	if headSize+len(compressed) >= len(raw) {
		return appendRawPayload(dst, raw)
	}

	dst = append(dst, byte(compression.Codec))
	dst = append(dst, head[:headSize]...)

	return append(dst, compressed...)
}

func appendRawPayload(dst, raw []byte) []byte {
	dst = append(dst, byte(CodecNone))

	return append(dst, raw...)
}

func DecodePayload(dst, payload []byte, version uint16, maxSize int) ([]byte, error) {
	if version < PayloadVersionCompressed {
		return payload, nil
	}

	if len(payload) < codecPrefixSize {
		return nil, fmt.Errorf("%w: payload needs %d byte codec prefix", ErrIncompleteFrame, codecPrefixSize)
	}

	id := CodecID(payload[0])
	body := payload[codecPrefixSize:]

	if id == CodecNone {
		return body, nil
	}

	impl, ok := codecs[id]
	if !ok {
		return nil, fmt.Errorf("%w: id %d", ErrUnknownCodec, id)
	}

	size, headSize := binary.Uvarint(body)
	if headSize <= 0 {
		return nil, fmt.Errorf("%w: malformed uncompressed size", ErrIncompleteFrame)
	}

	if maxSize > 0 && size > uint64(maxSize) {
		return nil, fmt.Errorf("%w: %d bytes (max %d)", ErrPayloadTooLarge, size, maxSize)
	}

	if maxSize > 0 && cap(dst) < int(size) {
		dst = make([]byte, 0, size)
	}

	result, err := impl.decompress(dst, body[headSize:])
	if err != nil {
		return nil, fmt.Errorf("decompress payload: %w", err)
	}

	if uint64(len(result)) != size {
		return nil, fmt.Errorf("%w: declared %d bytes, got %d", ErrIncompleteFrame, size, len(result))
	}

	return result, nil
}
