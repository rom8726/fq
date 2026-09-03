package format

import (
	"errors"
	"fmt"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

type CodecID uint8

const (
	CodecNone CodecID = 0
	CodecS2   CodecID = 1
	CodecZstd CodecID = 2
)

var ErrUnknownCodec = errors.New("unknown compression codec")

type Compression struct {
	Codec        CodecID
	MinFrameSize int
}

func (c Compression) Enabled() bool {
	return c.Codec != CodecNone
}

type codec interface {
	compress(dst, src []byte) []byte
	decompress(dst, src []byte) ([]byte, error)
}

type s2Codec struct{}

func (s2Codec) compress(dst, src []byte) []byte {
	return s2.Encode(dst, src)
}

func (s2Codec) decompress(dst, src []byte) ([]byte, error) {
	return s2.Decode(dst, src)
}

type zstdCodec struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func (c zstdCodec) compress(dst, src []byte) []byte {
	return c.encoder.EncodeAll(src, dst[:0])
}

func (c zstdCodec) decompress(dst, src []byte) ([]byte, error) {
	return c.decoder.DecodeAll(src, dst[:0])
}

var codecs = newCodecRegistry()

func newCodecRegistry() map[CodecID]codec {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(fmt.Sprintf("init zstd encoder: %v", err))
	}

	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic(fmt.Sprintf("init zstd decoder: %v", err))
	}

	return map[CodecID]codec{
		CodecS2:   s2Codec{},
		CodecZstd: zstdCodec{encoder: encoder, decoder: decoder},
	}
}

func ParseCodec(name string) (CodecID, error) {
	switch name {
	case "", "none":
		return CodecNone, nil
	case "s2":
		return CodecS2, nil
	case "zstd":
		return CodecZstd, nil
	default:
		return CodecNone, fmt.Errorf("%w: %q", ErrUnknownCodec, name)
	}
}

func (c CodecID) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecS2:
		return "s2"
	case CodecZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

func SupportedCodecs() []uint8 {
	return []uint8{uint8(CodecS2), uint8(CodecZstd)}
}
