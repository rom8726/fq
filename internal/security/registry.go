package security

import (
	"crypto/sha256"
	"crypto/subtle"
)

type registryEntry struct {
	digest [sha256.Size]byte
	role   Role
}

type Registry struct {
	entries []registryEntry
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Add(token string, role Role) error {
	if role == RoleNone {
		return ErrUnknownRole
	}

	digest := sha256.Sum256([]byte(token))
	for i := range r.entries {
		if r.entries[i].digest == digest {
			return ErrDuplicateToken
		}
	}

	r.entries = append(r.entries, registryEntry{digest: digest, role: role})

	return nil
}

func (r *Registry) Enabled() bool {
	return r != nil && len(r.entries) > 0
}

func (r *Registry) Authenticate(token string) (Role, bool) {
	if !r.Enabled() {
		return RoleNone, false
	}

	digest := sha256.Sum256([]byte(token))
	matched := 0
	found := 0

	for i := range r.entries {
		equal := subtle.ConstantTimeCompare(r.entries[i].digest[:], digest[:])
		matched |= equal
		found = subtle.ConstantTimeSelect(equal, int(r.entries[i].role), found)
	}

	if matched != 1 {
		return RoleNone, false
	}

	return Role(found), true
}
