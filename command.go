package wire

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/lib/pq/oid"
	"github.com/stackql/psql-wire/codes"
	psqlerr "github.com/stackql/psql-wire/errors"
	"github.com/stackql/psql-wire/internal/buffer"
	"github.com/stackql/psql-wire/internal/types"
	"github.com/stackql/psql-wire/pkg/sqldata"
	"go.uber.org/zap"
)

// NewErrUnimplementedMessageType is called whenever a unimplemented message
// type is send. This error indicates to the client that the send message cannot
// be processed at this moment in time.
func NewErrUnimplementedMessageType(t types.ClientMessage) error {
	err := fmt.Errorf("unimplemented client message type: %d", t)
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.ConnectionDoesNotExist), psqlerr.LevelFatal)
}

type SimpleQueryFn func(ctx context.Context, query string, writer DataWriter) error

type CloseFn func(ctx context.Context) error

// consumeCommands consumes incoming commands send over the Postgres wire connection.
// Commands consumed from the connection are returned through a go channel.
// Responses for the given message type are written back to the client.
// This method keeps consuming messages until the client issues a close message
// or the connection is terminated.
func (srv *Server) consumeCommands(ctx context.Context, conn SQLConnection) (err error) {
	srv.logger.Debug("ready for query... starting to consume commands")

	// TODO(Jeroen): include a indentification value inside the context that
	// could be used to identify connections at a later stage.

	for {
		err = readyForQuery(conn, types.ServerIdle)
		if err != nil {
			return err
		}

		t, length, err := conn.ReadTypedMsg()
		if err == io.EOF {
			return nil
		}

		// NOTE(Jeroen): we could recover from this scenario
		if errors.Is(err, buffer.ErrMessageSizeExceeded) {
			err = srv.handleMessageSizeExceeded(conn, conn, err)
			if err != nil {
				return err
			}

			continue
		}

		srv.logger.Debug("incoming command", zap.Int("length", length), zap.String("type", string(t)))

		if err != nil {
			return err
		}

		err = srv.handleCommand(ctx, conn, t)
		if err != nil {
			return err
		}

		// NOTE(Prasad): If conn is closed after handleCommand return.
		if t == types.ClientClose || t == types.ClientTerminate {
			return nil
		}
	}
}

// handleMessageSizeExceeded attempts to unwrap the given error message as
// message size exceeded. The expected message size will be consumed and
// discarded from the given reader. An error message is written to the client
// once the expected message size is read.
//
// The given error is returned if it does not contain an message size exceeded
// type. A fatal error is returned when an unexpected error is returned while
// consuming the expected message size or when attempting to write the error
// message back to the client.
func (srv *Server) handleMessageSizeExceeded(reader buffer.Reader, writer buffer.Writer, exceeded error) (err error) {
	unwrapped, has := buffer.UnwrapMessageSizeExceeded(exceeded)
	if !has {
		return exceeded
	}

	err = reader.Slurp(unwrapped.Size)
	if err != nil {
		return err
	}

	return ErrorCode(writer, exceeded)
}

// handleCommand handles the given client message. A client message includes a
// message type and reader buffer containing the actual message. The type
// indecates a action executed by the client.
func (srv *Server) handleCommand(ctx context.Context, conn SQLConnection, t types.ClientMessage) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch t {
	case types.ClientSync:
		// TODO(Jeroen): client sync received
		return srv.completeSuccess(ctx, conn)
	case types.ClientSimpleQuery:
		// TODO: make this a function of connection
		return srv.handleSimpleQuery(ctx, conn)
	case types.ClientExecute:
		return srv.completeSuccess(ctx, conn)
	case types.ClientParse:
		return srv.completeSuccess(ctx, conn)
	case types.ClientDescribe:
		return srv.completeSuccess(ctx, conn)
	case types.ClientBind:
		return srv.completeSuccess(ctx, conn)
	case types.ClientFlush:
		return srv.completeSuccess(ctx, conn)
	case types.ClientCopyData, types.ClientCopyDone, types.ClientCopyFail:
		// We're supposed to ignore these messages, per the protocol spec. This
		// state will happen when an error occurs on the server-side during a copy
		// operation: the server will send an error and a ready message back to
		// the client, and must then ignore further copy messages. See:
		// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
		break
	case types.ClientClose:
		err = srv.handleConnClose(ctx)
		if err != nil {
			return err
		}

		return conn.Close()
	case types.ClientTerminate:
		err = srv.handleConnTerminate(ctx)
		if err != nil {
			return err
		}

		return conn.Close()
	default:
		return ErrorCode(conn, NewErrUnimplementedMessageType(t))
	}

	return nil
}

func (srv *Server) completeSuccess(ctx context.Context, cn SQLConnection) error {
	dw := &dataWriter{
		ctx:    ctx,
		client: cn,
	}
	dw.Complete("", "OK")
	return nil
}

func (srv *Server) handleSimpleQuery(ctx context.Context, cn SQLConnection) error {
	if srv.SimpleQuery == nil && srv.SQLBackendFactory == nil {
		return ErrorCode(cn, NewErrUnimplementedMessageType(types.ClientSimpleQuery))
	}

	query, err := cn.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming query", zap.String("query", query))

	if cn.HasSQLBackend() {
		qArr, err := cn.SplitCompoundQuery(query)
		if err != nil {
			return err
		}
		for i, q := range qArr {
			if q == "" {
				if i == len(qArr)-1 {
					// trailing semicolon, ignore
					srv.completeSuccess(ctx, cn)
					return nil
				}
				continue
			}
			rdr, err := cn.HandleSimpleQuery(ctx, q)
			if err != nil {
				return ErrorCode(cn, err)
			}
			dw := &dataWriter{
				ctx:    ctx,
				client: cn,
			}
			var headersWritten bool
			for {
				if rdr == nil {
					dw.Complete("", "OK")
					return nil
				}
				res, err := rdr.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						notices := cn.GetDebugStr()
						if res == nil {
							dw.Complete(notices, "OK")
							return nil
						}
						if !headersWritten {
							headersWritten = true
							srv.writeSQLResultHeader(ctx, res, dw)
						}
						srv.writeSQLResultRows(ctx, res, dw)
						// TODO: add debug messages, configurably
						dw.Complete(notices, "OK")
						return nil
					}
					return ErrorCode(cn, err)
				}
				if !headersWritten {
					headersWritten = true
					dw.Define(nil)
				}
				srv.writeSQLResultRows(ctx, res, dw)
			}
		}
	}

	err = srv.SimpleQuery(ctx, query, &dataWriter{
		ctx:    ctx,
		client: cn,
	})

	if err != nil {
		return ErrorCode(cn, err)
	}

	return nil
}

func (srv *Server) writeSQLResultRows(ctx context.Context, res sqldata.ISQLResult, writer DataWriter) error {
	for _, r := range res.GetRows() {
		writer.Row(r.GetRowDataForPgWire())
	}
	return nil
}

func (srv *Server) writeSQLResultHeader(ctx context.Context, res sqldata.ISQLResult, writer DataWriter) error {
	var colz Columns
	for _, c := range res.GetColumns() {
		colz = append(colz,
			Column{
				Table:  c.GetTableId(),
				Name:   c.GetName(),
				Oid:    oid.Oid(c.GetObjectID()),
				Width:  c.GetWidth(),
				Format: TextFormat,
			},
		)
	}
	return writer.Define(colz)
}

func (srv *Server) handleConnClose(ctx context.Context) error {
	if srv.CloseConn == nil {
		return nil
	}

	return srv.CloseConn(ctx)
}

func (srv *Server) handleConnTerminate(ctx context.Context) error {
	if srv.TerminateConn == nil {
		return nil
	}

	return srv.TerminateConn(ctx)
}
