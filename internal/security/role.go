package security

import "strings"

type Role uint8

const (
	RoleNone Role = iota
	RoleRO
	RoleRW
	RoleAdmin
)

func ParseRole(value string) (Role, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "ro":
		return RoleRO, nil
	case "rw":
		return RoleRW, nil
	case "admin":
		return RoleAdmin, nil
	default:
		return RoleNone, ErrUnknownRole
	}
}

func (r Role) String() string {
	switch r {
	case RoleRO:
		return "ro"
	case RoleRW:
		return "rw"
	case RoleAdmin:
		return "admin"
	case RoleNone:
		return "none"
	default:
		return "none"
	}
}

func (r Role) Allows(required Role) bool {
	return r != RoleNone && r >= required
}
