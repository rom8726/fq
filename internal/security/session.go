package security

import "context"

const MaxAuthFailures = 5

type Session struct {
	registry *Registry
	role     Role
	failures int
}

func NewSession(registry *Registry) *Session {
	return &Session{registry: registry}
}

func (s *Session) Enabled() bool {
	return s != nil && s.registry.Enabled()
}

func (s *Session) Role() Role {
	if !s.Enabled() {
		return RoleAdmin
	}

	return s.role
}

func (s *Session) Failures() int {
	if s == nil {
		return 0
	}

	return s.failures
}

func (s *Session) Authenticate(token string) error {
	if !s.Enabled() {
		return nil
	}

	role, ok := s.registry.Authenticate(token)
	if !ok {
		s.failures++
		if s.failures >= MaxAuthFailures {
			return ErrTooManyAuthFailures
		}

		return ErrAuthenticationFailed
	}

	s.role = role
	s.failures = 0

	return nil
}

func (s *Session) Authorize(required Role) error {
	if !s.Enabled() {
		return nil
	}

	if s.role == RoleNone {
		return ErrNotAuthenticated
	}

	if !s.role.Allows(required) {
		return ErrPermissionDenied
	}

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
