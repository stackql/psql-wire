package wire

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/jeroenrinzema/psql-wire/pkg/sqlbackend"
	"go.uber.org/zap"
)

// OptionFn options pattern used to define and set options for the given
// PostgreSQL server.
type OptionFn func(*Server)

// SimpleQuery sets the simple query handle inside the given server instance.
func SimpleQuery(fn SimpleQueryFn) OptionFn {
	return func(srv *Server) {
		srv.SimpleQuery = fn
	}
}

// SQLBackend sets the SQL Backend object to
// handle queries inside the given server instance.
func SQLBackend(sb sqlbackend.ISQLBackend) OptionFn {
	return func(srv *Server) {
		srv.SQLBackend = sb
	}
}

// CloseConn sets the close connection handle inside the given server instance.
func CloseConn(fn CloseFn) OptionFn {
	return func(srv *Server) {
		srv.CloseConn = fn
	}
}

// TerminateConn sets the terminate connection handle inside the given server instance.
func TerminateConn(fn CloseFn) OptionFn {
	return func(srv *Server) {
		srv.TerminateConn = fn
	}
}

// MessageBufferSize sets the message buffer size which is allocated once a new
// connection gets constructed. If a negative value or zero value is provided is
// the default message buffer size used.
func MessageBufferSize(size int) OptionFn {
	return func(srv *Server) {
		srv.BufferedMsgSize = size
	}
}

// Certificates sets the given TLS certificates to be used to initialize a
// secure connection between the front-end (client) and back-end (server).
func Certificates(certs []tls.Certificate) OptionFn {
	return func(srv *Server) {
		srv.Certificates = certs
	}
}

// ClientCAs sets the given Client CAs to be used, by the server, to verify a
// secure connection between the front-end (client) and back-end (server).
func ClientCAs(cas *x509.CertPool) OptionFn {
	return func(srv *Server) {
		srv.ClientCAs = cas
	}
}

// ClientAuth sets the given Client Auth to be used, by the server, to verify a
// secure connection between the front-end (client) and back-end (server).
func ClientAuth(authType tls.ClientAuthType) OptionFn {
	return func(srv *Server) {
		srv.ClientAuth = authType
	}
}

// GlobalParameters sets the server parameters which are send back to the
// front-end (client) once a handshake has been established.
func GlobalParameters(params Parameters) OptionFn {
	return func(srv *Server) {
		srv.Parameters = params
	}
}

// Logger sets the given zap logger as the default logger for the given server.
func Logger(logger *zap.Logger) OptionFn {
	return func(srv *Server) {
		srv.logger = logger
	}
}
