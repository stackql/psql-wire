package wire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

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

// errExtendedQueryError is a sentinel error returned by extended query handlers
// when they have already sent an ErrorResponse to the client. The command loop
// uses this to enter error state: all subsequent messages are discarded until
// Sync, which sends ReadyForQuery with a failed-transaction status.
var errExtendedQueryError = errors.New("extended query error")

type SimpleQueryFn func(ctx context.Context, query string, writer DataWriter) error

type CloseFn func(ctx context.Context) error

// consumeCommands consumes incoming commands sent over the Postgres wire connection.
// Commands consumed from the connection are returned through a go channel.
// Responses for the given message type are written back to the client.
// This method keeps consuming messages until the client issues a terminate message
// or the connection is terminated.
//
// ReadyForQuery is sent once initially, then only after SimpleQuery completion
// and Sync messages (per the PostgreSQL extended query protocol).
func (srv *Server) consumeCommands(ctx context.Context, conn SQLConnection) (err error) {
	srv.logger.Debug("ready for query... starting to consume commands")

	// Send initial ReadyForQuery
	err = readyForQuery(conn, types.ServerIdle)
	if err != nil {
		return err
	}

	// inErrorState tracks whether an extended query handler has failed.
	// Per the PostgreSQL protocol, after an error in extended query mode,
	// all subsequent messages are discarded until a Sync message is received.
	inErrorState := false

	for {
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

			// Send ReadyForQuery after error recovery
			err = readyForQuery(conn, types.ServerIdle)
			if err != nil {
				return err
			}

			continue
		}

		srv.logger.Debug("incoming command", zap.Int("length", length), zap.String("type", string(t)))

		if err != nil {
			return err
		}

		// When in error state, discard all messages except Sync and Terminate.
		// Sync resets the error state; Terminate always closes the connection.
		if inErrorState {
			switch t {
			case types.ClientSync:
				inErrorState = false
				err = readyForQuery(conn, types.ServerTransactionFailed)
				if err != nil {
					return err
				}
			case types.ClientTerminate:
				err = srv.handleCommand(ctx, conn, t)
				if err != nil {
					return err
				}
			default:
				srv.logger.Debug("discarding message in error state", zap.String("type", string(t)))
			}
			continue
		}

		err = srv.handleCommand(ctx, conn, t)
		if errors.Is(err, errExtendedQueryError) {
			inErrorState = true
			continue
		}
		if err != nil {
			return err
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
// indicates an action executed by the client.
//
// For the extended query protocol, ReadyForQuery is NOT sent after each message.
// It is only sent by handleSync (after Sync) and handleSimpleQuery (after SimpleQuery).
func (srv *Server) handleCommand(ctx context.Context, conn SQLConnection, t types.ClientMessage) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch t {
	case types.ClientSimpleQuery:
		return srv.handleSimpleQuery(ctx, conn)
	case types.ClientParse:
		return srv.handleParse(ctx, conn)
	case types.ClientBind:
		return srv.handleBind(ctx, conn)
	case types.ClientDescribe:
		return srv.handleDescribe(ctx, conn)
	case types.ClientExecute:
		return srv.handleExecute(ctx, conn)
	case types.ClientSync:
		return srv.handleSync(ctx, conn)
	case types.ClientFlush:
		return srv.handleFlush(ctx, conn)
	case types.ClientClose:
		return srv.handleClose(ctx, conn)
	case types.ClientCopyData, types.ClientCopyDone, types.ClientCopyFail:
		// We're supposed to ignore these messages, per the protocol spec. This
		// state will happen when an error occurs on the server-side during a copy
		// operation: the server will send an error and a ready message back to
		// the client, and must then ignore further copy messages. See:
		// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
		break
	case types.ClientTerminate:
		err = srv.handleConnClose(ctx)
		if err != nil {
			return err
		}

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

func (srv *Server) handleSimpleQuery(ctx context.Context, cn SQLConnection) error {
	if srv.SimpleQuery == nil && srv.SQLBackendFactory == nil {
		ErrorCode(cn, NewErrUnimplementedMessageType(types.ClientSimpleQuery))
		return readyForQuery(cn, types.ServerIdle)
	}

	query, err := cn.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming query", zap.String("query", query))

	if isEmptyQuery(query) {
		if err = emptyQuery(cn); err != nil {
			return err
		}
		return readyForQuery(cn, types.ServerIdle)
	}

	if cn.HasSQLBackend() {
		qArr, err := cn.SplitCompoundQuery(query)
		if err != nil {
			return err
		}
		for i, q := range qArr {
			if q == "" {
				if i == len(qArr)-1 {
					// trailing semicolon, ignore
					commandComplete(cn, "OK")
					return readyForQuery(cn, types.ServerIdle)
				}
				continue
			}
			rdr, err := cn.HandleSimpleQuery(ctx, q)
			if err != nil {
				ErrorCode(cn, err)
				return readyForQuery(cn, types.ServerIdle)
			}
			dw := &dataWriter{
				ctx:    ctx,
				client: cn,
			}
			err = srv.writeSQLResultStream(rdr, dw, nil)
			if err != nil {
				if writeErr := ErrorCode(cn, err); writeErr != nil {
					return writeErr
				}
				return readyForQuery(cn, types.ServerIdle)
			}
			err = dw.Complete(cn.GetDebugStr(), "OK")
			if err != nil {
				return err
			}
			return readyForQuery(cn, types.ServerIdle)
		}
	}

	err = srv.SimpleQuery(ctx, query, &dataWriter{
		ctx:    ctx,
		client: cn,
	})

	if err != nil {
		ErrorCode(cn, err)
		return readyForQuery(cn, types.ServerIdle)
	}

	return readyForQuery(cn, types.ServerIdle)
}

func isEmptyQuery(query string) bool {
	return strings.Trim(query, " \t\r\n;") == ""
}

func (srv *Server) writeSQLResultStream(
	stream sqldata.ISQLResultStream,
	writer *dataWriter,
	resultFormats []int16,
) error {
	if stream == nil {
		return nil
	}
	if columns := stream.GetColumns(); columns != nil && writer.columns == nil {
		if err := writer.Define(sqlColumns(columns, resultFormats)); err != nil {
			return err
		}
	}
	for {
		result, err := stream.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if result != nil {
			if writeErr := srv.writeSQLResult(result, writer, resultFormats); writeErr != nil {
				return writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func (srv *Server) writeSQLResult(result sqldata.ISQLResult, writer *dataWriter, resultFormats []int16) error {
	if writer.columns == nil {
		if err := writer.Define(sqlColumns(result.GetColumns(), resultFormats)); err != nil {
			return err
		}
	}
	for _, row := range result.GetRows() {
		if err := writer.Row(row.GetRowDataForPgWire()); err != nil {
			return err
		}
	}
	return nil
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
