package database

import (
	"context"
	"strconv"

	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/protocol"
	"github.com/fq-db/fq/internal/security"
)

const helloArgumentsWithAuth = 3

func (d *Database) handleHelloQuery(ctx context.Context, query compute.Query, dst []byte) ([]byte, error) {
	version, err := strconv.ParseUint(query.Arg(0), 10, 16)
	if err != nil {
		return d.appendErrorMsg(dst, compute.ErrInvalidArguments), nil
	}

	if negotiateErr := protocol.SessionFrom(ctx).Negotiate(uint16(version)); negotiateErr != nil {
		return d.appendErrorMsg(dst, negotiateErr), nil
	}

	session := security.SessionFrom(ctx)

	if query.ArgumentCount() == helloArgumentsWithAuth {
		authQuery := compute.NewQuery(compute.AuthCommandID, []string{query.Arg(2)})

		authResponse, authErr := d.handleAuthQuery(session, authQuery, dst)
		if authErr != nil {
			return nil, authErr
		}

		if isErrorResponse(authResponse) {
			return authResponse, nil
		}
	}

	return d.appendServerInfo(dst, session), nil
}

func (d *Database) appendServerInfo(dst []byte, session *security.Session) []byte {
	dst = protocol.AppendOK(dst)
	dst = strconv.AppendUint(dst, uint64(protocol.CurrentVersion), 10)
	dst = append(dst, ';')
	dst = strconv.AppendInt(dst, int64(d.maxMessageSize), 10)
	dst = append(dst, ';')

	if session.Enabled() {
		dst = append(dst, '1')
	} else {
		dst = append(dst, '0')
	}

	dst = append(dst, ';')
	dst = append(dst, session.Role().String()...)

	return dst
}

func isErrorResponse(response []byte) bool {
	kind, _, _ := protocol.ParseResponse(response)

	return kind == protocol.KindError
}
