package wire

import (
	"context"
	"errors"

	"github.com/stackql/psql-wire/internal/buffer"
	"github.com/stackql/psql-wire/internal/types"
)

// DataWriter represents a writer interface for writing columns and data rows
// using the Postgres wire to the connected client.
type DataWriter interface {
	// Define writes the column headers containing their type definitions, width
	// type oid, etc. to the underlaying Postgres client. The column headers
	// could only be written once. An error will be returned whenever this
	// method is called twice.
	Define(Columns) error
	// Row writes a single data row containing the values inside the given slice to
	// the underlaying Postgres client. The column headers have to be written before
	// sending rows. Each item inside the slice represents a single column value.
	// The slice length needs to be the same length as the defined columns. Nil
	// values are encoded as NULL values.
	Row([]interface{}) error

	// Empty announces to the client a empty response and that no data rows should
	// be expected.
	Empty() error

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	Complete(notices string, description string) error
}

// ErrColumnsDefined is thrown when columns already have been defined inside the
// given data writer.
var ErrColumnsDefined = errors.New("columns have already been defined")

// ErrUndefinedColumns is thrown when the columns inside the data writer have not
// yet been defined.
var ErrUndefinedColumns = errors.New("columns have not been defined")

// ErrDataWritten is thrown when an empty result is attempted to be send to the
// client while data has already been written.
var ErrDataWritten = errors.New("data has already been written")

// ErrClosedWriter is thrown when the data writer has been closed
var ErrClosedWriter = errors.New("closed writer")

// dataWriter is a implementation of the DataWriter interface.
type dataWriter struct {
	columns Columns
	ctx     context.Context
	client  buffer.Writer
	closed  bool
	written uint64
}

func (writer *dataWriter) Define(columns Columns) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns != nil {
		return ErrColumnsDefined
	}

	writer.columns = columns
	return writer.columns.Define(writer.ctx, writer.client)
}

func (writer *dataWriter) Row(values []interface{}) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	writer.written++

	return writer.columns.Write(writer.ctx, writer.client, values)
}

func (writer *dataWriter) Empty() error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	if writer.written != 0 {
		return ErrDataWritten
	}

	defer writer.close()
	return emptyQuery(writer.client)
}

func (writer *dataWriter) ServerReady() error {
	if writer.closed {
		return ErrClosedWriter
	}
	defer writer.close()
	return serverReady(writer.client)
}

func (writer *dataWriter) Complete(notices, description string) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.written == 0 && writer.columns != nil {
		err := writer.Empty()
		if err != nil {
			return err
		}
	}

	defer writer.close()
	if notices != "" {
		noticesComplete(writer.client, notices)
		return commandComplete(writer.client, "")
	}
	return commandComplete(writer.client, description)
}

func (writer *dataWriter) close() {
	writer.closed = true
}

// commandComplete announces that the requested command has successfully been executed.
// The given description is written back to the client and could be used to send
// additional meta data to the user.
func commandComplete(writer buffer.Writer, description string) error {
	writer.Start(types.ServerCommandComplete)
	writer.AddString(description)
	writer.AddNullTerminate()
	return writer.End()
}

func serverReady(writer buffer.Writer) error {
	writer.Start(types.ServerReady)
	writer.AddByte('I') // idle
	writer.AddNullTerminate()
	return writer.End()
}

// emulating the postgres backend, per [send_message_to_frontend()](https://github.com/postgres/postgres/blob/4694aedf63bf5b5d91f766cb6d6d6d14a9e4434b/src/backend/utils/error/elog.c#L3516)
func noticesComplete(writer buffer.Writer, notices string) error {
	writer.Start(types.ServerNoticeResponse)
	// writer.AddInt32(int32(len(notices) + 7)) // length
	writer.AddByte('S') // code
	writer.AddString("NOTICE")
	writer.AddNullTerminate()
	writer.AddByte('V') // code
	writer.AddString("NOTICE")
	writer.AddNullTerminate()
	writer.AddByte('C') // code
	// per https://www.postgresql.org/docs/current/errcodes-appendix.html
	writer.AddString("01000") // warning
	writer.AddNullTerminate()
	writer.AddByte('M')
	writer.AddString("a notice level event has occurred")
	writer.AddNullTerminate()
	writer.AddByte('D') // code
	writer.AddString(notices)
	writer.AddNullTerminate()
	writer.AddNullTerminate()
	return writer.End()
}

// emptyQuery indicates a empty query response by sending a emptyQuery message.
func emptyQuery(writer buffer.Writer) error {
	writer.Start(types.ServerEmptyQuery)
	return writer.End()
}
