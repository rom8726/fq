package protocol

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
)

const (
	OKPrefix    = "ok|"
	ErrorPrefix = "err|"
	NextPrefix  = "nxt|"

	serverInfoFields = 4
)

type Kind uint8

const (
	KindOK Kind = iota
	KindNext
	KindError
)

var (
	ErrMalformedResponse      = errors.New("malformed response frame")
	ErrUnexpectedContinuation = errors.New("unexpected continuation frame")
)

type ServerInfo struct {
	Version        uint16
	MaxMessageSize int
	AuthRequired   bool
	Role           string
}

func AppendOK(dst []byte) []byte {
	return append(dst, OKPrefix...)
}

func AppendNext(dst []byte) []byte {
	return append(dst, NextPrefix...)
}

func ParseResponse(frame []byte) (Kind, []byte, error) {
	switch {
	case bytes.HasPrefix(frame, []byte(OKPrefix)):
		return KindOK, frame[len(OKPrefix):], nil
	case bytes.HasPrefix(frame, []byte(NextPrefix)):
		return KindNext, frame[len(NextPrefix):], nil
	case bytes.HasPrefix(frame, []byte(ErrorPrefix)):
		return parseErrorFrame(frame[len(ErrorPrefix):])
	default:
		return KindOK, nil, ErrMalformedResponse
	}
}

func parseErrorFrame(rest []byte) (Kind, []byte, error) {
	separator := bytes.IndexByte(rest, '|')
	if separator < 0 {
		return KindError, nil, ErrMalformedResponse
	}

	value, err := strconv.ParseUint(string(rest[:separator]), 10, 16)
	if err != nil {
		return KindError, nil, ErrMalformedResponse
	}

	return KindError, nil, NewError(Code(value), string(rest[separator+1:]))
}

func ParseServerInfo(body []byte) (ServerInfo, error) {
	fields := strings.Split(string(body), ";")
	if len(fields) != serverInfoFields {
		return ServerInfo{}, ErrMalformedResponse
	}

	version, err := strconv.ParseUint(fields[0], 10, 16)
	if err != nil {
		return ServerInfo{}, ErrMalformedResponse
	}

	maxMessageSize, err := strconv.Atoi(fields[1])
	if err != nil {
		return ServerInfo{}, ErrMalformedResponse
	}

	return ServerInfo{
		Version:        uint16(version),
		MaxMessageSize: maxMessageSize,
		AuthRequired:   fields[2] == "1",
		Role:           fields[3],
	}, nil
}
