package network

import (
	"context"
	"crypto/tls"
	"net"
)

type ServerOption func(*TCPServer)

type clientOptions struct {
	tlsConfig *tls.Config
}

type ClientOption func(*clientOptions)

func WithTLS(config *tls.Config) ServerOption {
	return func(server *TCPServer) {
		server.tlsConfig = config
	}
}

func WithConnContext(fn func(context.Context, net.Conn) context.Context) ServerOption {
	return func(server *TCPServer) {
		server.connContext = fn
	}
}

func WithClientTLS(config *tls.Config) ClientOption {
	return func(options *clientOptions) {
		options.tlsConfig = config
	}
}
