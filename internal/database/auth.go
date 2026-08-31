package database

import (
	"errors"
	"strings"

	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/security"
)

const authFailurePort = "client"

var commandRoles = map[compute.CommandID]security.Role{
	compute.GetCommandID:      security.RoleRO,
	compute.ScanCommandID:     security.RoleRO,
	compute.PScanCommandID:    security.RoleRO,
	compute.WatchCommandID:    security.RoleRO,
	compute.StreamCommandID:   security.RoleRO,
	compute.PStreamCommandID:  security.RoleRO,
	compute.QStreamCommandID:  security.RoleRO,
	compute.QPStreamCommandID: security.RoleRO,
	compute.IncrCommandID:     security.RoleRW,
	compute.DelCommandID:      security.RoleRW,
	compute.MDelCommandID:     security.RoleRW,
	compute.RLimitCommandID:   security.RoleRW,
	compute.QuotaCommandID:    security.RoleRW,
	compute.FlushDBCommandID:  security.RoleAdmin,
	compute.TruncateCommandID: security.RoleAdmin,
	compute.InspectCommandID:  security.RoleAdmin,
}

func requiresAuthorization(commandID compute.CommandID) bool {
	switch commandID {
	case compute.AuthCommandID, compute.MsgSizeCommandID:
		return false
	default:
		return true
	}
}

func commandRole(query compute.Query) security.Role {
	if query.CommandID() == compute.QuotaCommandID {
		arguments := query.Arguments()
		if len(arguments) > 0 && strings.EqualFold(arguments[0], "INF") {
			return security.RoleRO
		}

		return security.RoleRW
	}

	role, found := commandRoles[query.CommandID()]
	if !found {
		return security.RoleAdmin
	}

	return role
}

func (d *Database) handleAuthQuery(
	session *security.Session,
	query compute.Query,
	dst []byte,
) ([]byte, error) {
	arguments := query.Arguments()
	if len(arguments) != 1 {
		return appendErrorMsg(dst, errInvalidArgumentsCount), nil
	}

	if err := session.Authenticate(arguments[0]); err != nil {
		observability.IncAuthFailures(authFailurePort)

		d.logger.Warn().Int("failures", session.Failures()).Msg("client authentication failed")

		if errors.Is(err, security.ErrTooManyAuthFailures) {
			return nil, err
		}

		return appendErrorMsg(dst, err), nil
	}

	d.logger.Info().Str("role", session.Role().String()).Msg("client authenticated")

	return append(dst, okTrueMsg...), nil
}

func redactQuery(query string) string {
	trimmed := strings.TrimLeft(query, " \t\r\n")
	if len(trimmed) < len(compute.AuthCommand) {
		return query
	}

	if !strings.EqualFold(trimmed[:len(compute.AuthCommand)], compute.AuthCommand) {
		return query
	}

	rest := trimmed[len(compute.AuthCommand):]
	if rest != "" && rest[0] != ' ' && rest[0] != '\t' {
		return query
	}

	return compute.AuthCommand + " [REDACTED]"
}
