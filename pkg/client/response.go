package client

import (
	"bytes"
	"errors"
	"strconv"
)

const (
	respDelimiter = '|'
)

var (
	ErrCorruptedResponse = errors.New("corrupted response")
	ErrUnknownRespStatus = errors.New("unknown response status")
)

var (
	statusOK    = "ok"
	statusError = "err"
)

type ResponseStatus uint8

const (
	ResponseStatusUnknown ResponseStatus = iota
	ResponseStatusSuccess
	ResponseStatusError
)

type responseStruct struct {
	status ResponseStatus
	value  uint64
	err    error
}

func parseResponse(resp []byte) (responseStruct, error) {
	idx := bytes.IndexByte(resp, respDelimiter)
	if idx == -1 {
		return responseStruct{}, ErrCorruptedResponse
	}

	status := string(resp[:idx])
	data := string(resp[idx+1:])
	switch status {
	case statusOK:
		v, err := strconv.ParseUint(data, 16, 64)
		if err != nil {
			return responseStruct{}, ErrCorruptedResponse
		}

		return responseStruct{status: ResponseStatusSuccess, value: v}, nil
	case statusError:
		return responseStruct{status: ResponseStatusError, err: errors.New(data)}, nil
	default:
		return responseStruct{}, ErrUnknownRespStatus
	}
}
