package wire

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/stackql/psql-wire/internal/buffer"
	"github.com/stackql/psql-wire/internal/types"
	"github.com/stackql/psql-wire/pkg/sqlbackend"
	"go.uber.org/zap"
)

// ListenAndServe opens a new Postgres server using the given address and
// default configurations. The given handler function is used to handle simple
// queries. This method should be used to construct a simple Postgres server for
// testing purposes or simple use cases.
func ListenAndServe(address string, handler SimpleQueryFn) error {
	server, err := NewServer(SimpleQuery(handler))
	if err != nil {
		return err
	}

	return server.ListenAndServe(address)
}

// NewServer constructs a new Postgres server using the given address and server options.
func NewServer(options ...OptionFn) (*Server, error) {
	srv := &Server{
		logger: logrus.StandardLogger(),
		closer: make(chan struct{}),
	}

	for _, option := range options {
		option(srv)
	}

	return srv, nil
}

// Server contains options for listening to an address.
type Server struct {
	wg                sync.WaitGroup
	logger            *logrus.Logger
	Auth              AuthStrategy
	BufferedMsgSize   int
	Parameters        Parameters
	Certificates      []tls.Certificate
	ClientCAs         *x509.CertPool
	ClientAuth        tls.ClientAuthType
	SimpleQuery       SimpleQueryFn
	SQLBackendFactory sqlbackend.SQLBackendFactory
	CloseConn         CloseFn
	TerminateConn     CloseFn
	closer            chan struct{}
}

func (srv *Server) CreateSQLBackend() (sqlbackend.ISQLBackend, error) {
	if srv.SQLBackendFactory == nil {
		return nil, fmt.Errorf("no sql backend factory provided")
	}

	return srv.SQLBackendFactory.NewSQLBackend()
}

// ListenAndServe opens a new Postgres server on the preconfigured address and
// starts accepting and serving incoming client connections.
func (srv *Server) ListenAndServe(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	return srv.Serve(listener)
}

// Serve accepts and serves incoming Postgres client connections using the
// preconfigured configurations. The given listener will be closed once the
// server is gracefully closed.
func (srv *Server) Serve(listener net.Listener) error {
	defer listener.Close()
	defer srv.logger.Info("closing server")

	srv.logger.Info("serving incoming connections", zap.String("addr", listener.Addr().String()))

	srv.wg.Add(1)

	// NOTE(Jeroen): handle graceful shutdowns
	go func() {
		defer srv.wg.Done()
		<-srv.closer

		err := listener.Close()
		if err != nil {
			srv.logger.Error("unexpected error while attempting to close the net listener", zap.Error(err))
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		srv.wg.Add(1)

		go func() {
			defer srv.wg.Done()
			ctx := context.Background()
			err = srv.serve(ctx, conn)
			if err != nil {
				srv.logger.Error("an unexpected error got returned while serving a client connection", zap.Error(err))
			}
		}()
	}
}

func (srv *Server) serve(ctx context.Context, conn net.Conn) error {
	ctx = setTypeInfo(ctx)
	defer conn.Close()

	srv.logger.Debug("serving a new client connection")

	conn, version, reader, err := srv.Handshake(conn)
	if err != nil {
		return err
	}

	if version == types.VersionCancel {
		return conn.Close()
	}

	srv.logger.Debug("handshake successful, validating authentication")

	writer := buffer.NewWriter(conn)
	ctx, err = srv.readParameters(ctx, reader)
	if err != nil {
		return err
	}

	err = srv.handleAuth(ctx, reader, writer)
	if err != nil {
		return err
	}

	srv.logger.Debug("connection authenticated, writing server parameters")

	ctx, err = srv.writeParameters(ctx, writer, srv.Parameters)
	if err != nil {
		return err
	}

	sqlBackend, err := srv.CreateSQLBackend()

	if err != nil {
		srv.logger.Debugf("no sql backend found, using default backend\n")
	}

	cn := NewSQLConnection(
		0,
		conn,
		reader,
		writer,
		sqlBackend,
	)

	// this should be a function of connection, not server
	return srv.consumeCommands(ctx, cn)
}

// Close gracefully closes the underlaying Postgres server.
func (srv *Server) Close() error {
	close(srv.closer)
	srv.wg.Wait()
	return nil
}
