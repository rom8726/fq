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
	case compute.AuthCommandID, compute.HelloCommandID:
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

func isReadOnlyQuery(query compute.Query) bool {
	switch query.CommandID() {
	case compute.GetCommandID,
		compute.ScanCommandID,
		compute.PScanCommandID,
		compute.WatchCommandID,
		compute.StreamCommandID,
		compute.PStreamCommandID,
		compute.QStreamCommandID,
		compute.QPStreamCommandID,
		compute.InspectCommandID:
		return true
	case compute.QuotaCommandID:
		return commandRole(query) == security.RoleRO
	default:
		return false
	}
}

func (d *Database) handleAuthQuery(
	session *security.Session,
	query compute.Query,
	dst []byte,
) ([]byte, error) {
	arguments := query.Arguments()
	if len(arguments) != 1 {
		return d.appendErrorMsg(dst, errInvalidArgumentsCount), nil
	}

	if err := session.Authenticate(arguments[0]); err != nil {
		observability.IncAuthFailures(authFailurePort)

		d.logger.Warn().Int("failures", session.Failures()).Msg("client authentication failed")

		if errors.Is(err, security.ErrTooManyAuthFailures) {
			return d.appendErrorMsg(dst, err), err
		}

		return d.appendErrorMsg(dst, err), nil
	}

	d.logger.Info().Str("role", session.Role().String()).Msg("client authenticated")

	return append(dst, okTrueMsg...), nil
}

func redactQuery(query string) string {
	fields := strings.Fields(query)
	if len(fields) == 0 {
		return query
	}

	switch {
	case strings.EqualFold(fields[0], compute.AuthCommand):
		return compute.AuthCommand + " [REDACTED]"
	case strings.EqualFold(fields[0], compute.HelloCommand) && len(fields) > 2:
		if len(fields) == 4 && strings.EqualFold(fields[2], compute.AuthCommand) {
			return strings.Join(fields[:3], " ") + " [REDACTED]"
		}

		return strings.Join(fields[:2], " ") + " [REDACTED]"
	default:
		return query
	}
}
