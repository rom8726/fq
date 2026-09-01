package protocol

import "context"

var (
	ErrHandshakeRequired        = NewError(CodeHandshakeRequired, "handshake required")
	ErrVersionAlreadyNegotiated = NewError(CodeVersionAlreadyNegotiated, "protocol version already negotiated")
)

type Session struct {
	version uint16
}

func NewSession() *Session {
	return &Session{}
}

func (s *Session) Version() uint16 {
	if s == nil {
		return 0
	}

	return s.version
}

func (s *Session) Negotiated() bool {
	if s == nil {
		return true
	}

	return s.version != 0
}

func (s *Session) Negotiate(version uint16) error {
	if s == nil {
		return nil
	}

	if s.version != 0 && s.version != version {
		return ErrVersionAlreadyNegotiated
	}

	if !IsSupported(version) {
		return Errorf(CodeUnsupportedVersion, "unsupported protocol version: %d", version)
	}

	s.version = version

	return nil
}

type sessionContextKey struct{}

func WithSession(ctx context.Context, session *Session) context.Context {
	return context.WithValue(ctx, sessionContextKey{}, session)
}

func SessionFrom(ctx context.Context) *Session {
	session, _ := ctx.Value(sessionContextKey{}).(*Session)

	return session
}
