package protocol

import (
	"errors"
	"fmt"
)

type Error struct {
	Code Code
	Msg  string
}

func NewError(code Code, msg string) *Error {
	return &Error{Code: code, Msg: msg}
}

func Errorf(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func (e *Error) Error() string {
	return e.Msg
}

func CodeOf(err error) (Code, bool) {
	var protoErr *Error
	if errors.As(err, &protoErr) {
		return protoErr.Code, true
	}

	return 0, false
}
