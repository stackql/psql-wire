package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stackql/psql-wire/internal/buffer"
	"github.com/stackql/psql-wire/internal/mock"
	"github.com/stackql/psql-wire/internal/types"
	"github.com/stackql/psql-wire/pkg/sqlbackend"
	"github.com/stackql/psql-wire/pkg/sqldata"
)

func schemaTestColumns() []sqldata.ISQLColumn {
	return []sqldata.ISQLColumn{
		sqldata.NewSQLColumn(sqldata.NewSQLTable(0, ""), "id", 0, 23, 4, -1, "text"),
	}
}

func schemaTestStream(kind string, columns []sqldata.ISQLColumn) sqldata.ISQLResultStream {
	switch kind {
	case "simple zero rows":
		return sqldata.NewSimpleSQLResultStream(sqldata.NewSQLResult(columns, 0, 0, nil))
	case "channel no results":
		stream := sqldata.NewChannelSQLResultStream(sqldata.NewSQLResult(columns, 0, 0, nil))
		_ = stream.Close()
		return stream
	default:
		stream := sqldata.NewChannelSQLResultStream()
		if kind == "channel rows" {
			stream = sqldata.NewChannelSQLResultStream(sqldata.NewSQLResult(columns, 0, 0, nil))
		}
		go func() {
			for _, value := range []int32{1, 2} {
				_ = stream.Write(sqldata.NewSQLResult(columns, 0, 0,
					[]sqldata.ISQLRow{sqldata.NewSQLRow([]interface{}{value})}))
			}
			_ = stream.Close()
		}()
		return stream
	}
}

func expectSchema(t *testing.T, client *mock.Client, format FormatCode) {
	t.Helper()
	expectMsg(t, client, types.ServerRowDescription)
	body := client.PeekMsg()
	if len(body) != 23 || binary.BigEndian.Uint16(body[:2]) != 1 ||
		string(body[2:5]) != "id\x00" || binary.BigEndian.Uint32(body[11:15]) != 23 ||
		binary.BigEndian.Uint16(body[21:23]) != uint16(format) {
		t.Fatalf("unexpected RowDescription: %x", body)
	}
}

func expectSchemaRows(t *testing.T, client *mock.Client, kind string, format FormatCode) {
	t.Helper()
	if kind != "channel rows" && kind != "legacy channel rows" {
		return
	}
	for _, value := range []int32{1, 2} {
		expectMsg(t, client, types.ServerDataRow)
		data := []byte{byte('0' + value)}
		if format == BinaryFormat {
			data = make([]byte, 4)
			binary.BigEndian.PutUint32(data, uint32(value))
		}
		body := client.PeekMsg()
		if len(body) != 6+len(data) || binary.BigEndian.Uint16(body[:2]) != 1 ||
			binary.BigEndian.Uint32(body[2:6]) != uint32(len(data)) || !bytes.Equal(body[6:], data) {
			t.Fatalf("unexpected DataRow: %x", body)
		}
	}
}

func TestSimpleQueryStreamSchema(t *testing.T) {
	for _, kind := range []string{"simple zero rows", "channel no results", "channel rows", "legacy channel rows"} {
		t.Run(kind, func(t *testing.T) {
			callback := func(context.Context, string) (sqldata.ISQLResultStream, error) {
				return schemaTestStream(kind, schemaTestColumns()), nil
			}
			server, err := NewServer(SQLBackendFactory(sqlbackend.NewSimpleSQLBackendFactory(callback)))
			if err != nil {
				t.Fatal(err)
			}
			client := connectAndHandshake(t, TListenAndServe(t, server))
			client.Start(types.ClientSimpleQuery)
			client.AddString("SELECT id")
			client.AddNullTerminate()
			if err := client.End(); err != nil {
				t.Fatal(err)
			}
			expectSchema(t, client, TextFormat)
			expectSchemaRows(t, client, kind, TextFormat)
			expectMsg(t, client, types.ServerCommandComplete)
			expectReadyForQuery(t, client, types.ServerIdle)
			client.Close(t)
		})
	}
}

type schemaTestBackend struct {
	sqlbackend.ISQLBackend
	sqlbackend.IExtendedQueryBackend
	columns []sqldata.ISQLColumn
}

func (backend *schemaTestBackend) HandleDescribePortal(
	context.Context, string, string, string, []uint32,
) ([]sqldata.ISQLColumn, error) {
	return backend.columns, nil
}

type schemaTestBackendFactory struct {
	backend sqlbackend.ISQLBackend
}

func (factory *schemaTestBackendFactory) NewSQLBackend() (sqlbackend.ISQLBackend, error) {
	return factory.backend, nil
}

func TestExtendedQueryStreamSchema(t *testing.T) {
	for _, description := range []string{"execute", "describe execute", "describe no data"} {
		for _, format := range []FormatCode{TextFormat, BinaryFormat} {
			for _, kind := range []string{"simple zero rows", "channel no results", "channel rows", "legacy channel rows"} {
				name := kind + "/" + description +
					"/" + map[FormatCode]string{TextFormat: "text", BinaryFormat: "binary"}[format]
				t.Run(name, func(t *testing.T) {
					columns := schemaTestColumns()
					callback := func(context.Context, string) (sqldata.ISQLResultStream, error) {
						return schemaTestStream(kind, columns), nil
					}
					simple := sqlbackend.NewSimpleSQLBackend(callback)
					backend := &schemaTestBackend{
						ISQLBackend:           simple,
						IExtendedQueryBackend: sqlbackend.NewDefaultExtendedQueryBackend(simple),
						columns:               columns,
					}
					if description == "describe no data" {
						backend.columns = nil
					}
					server, err := NewServer(SQLBackendFactory(&schemaTestBackendFactory{backend: backend}))
					if err != nil {
						t.Fatal(err)
					}
					client := connectAndHandshake(t, TListenAndServe(t, server))
					sendParse(t, client, "", "SELECT id", nil)
					expectMsg(t, client, types.ServerParseComplete)
					client.Start(types.ClientBind)
					client.AddString("")
					client.AddNullTerminate()
					client.AddString("")
					client.AddNullTerminate()
					client.AddInt16(0)
					client.AddInt16(0)
					client.AddInt16(1)
					client.AddInt16(int16(format))
					if err := client.End(); err != nil {
						t.Fatal(err)
					}
					expectMsg(t, client, types.ServerBindComplete)
					if description != "execute" {
						sendDescribePortal(t, client, "")
						if description == "describe no data" {
							expectMsg(t, client, types.ServerNoData)
						} else {
							expectSchema(t, client, format)
						}
					}
					sendExecute(t, client, "", 0)
					if description != "describe execute" {
						expectSchema(t, client, format)
					}
					expectSchemaRows(t, client, kind, format)
					expectMsg(t, client, types.ServerCommandComplete)
					sendSync(t, client)
					expectReadyForQuery(t, client, types.ServerIdle)
					client.Close(t)
				})
			}
		}
	}
}

func TestExplicitEmptyQueryResponse(t *testing.T) {
	var output bytes.Buffer
	writer := &dataWriter{ctx: context.Background(), client: buffer.NewWriter(&output), columns: Columns{}}
	if err := writer.Empty(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), []byte{'I', 0, 0, 0, 4}) {
		t.Fatalf("unexpected empty query response: %x", output.Bytes())
	}
	if err := writer.Complete("", "OK"); !errors.Is(err, ErrClosedWriter) {
		t.Fatalf("Complete after Empty = %v, want ErrClosedWriter", err)
	}
}

type schemaOnlyStream struct {
	columns []sqldata.ISQLColumn
	output  *bytes.Buffer
	t       *testing.T
}

func (stream *schemaOnlyStream) GetColumns() []sqldata.ISQLColumn { return stream.columns }
func (stream *schemaOnlyStream) Write(sqldata.ISQLResult) error   { return errors.New("not supported") }
func (stream *schemaOnlyStream) Close() error                     { return nil }
func (stream *schemaOnlyStream) Read() (sqldata.ISQLResult, error) {
	if stream.output.Len() == 0 || stream.output.Bytes()[0] != byte(types.ServerRowDescription) {
		stream.t.Fatal("stream was read before RowDescription was written")
	}
	return nil, io.EOF
}

func TestStreamSchemaWrittenBeforeRead(t *testing.T) {
	var output bytes.Buffer
	stream := &schemaOnlyStream{columns: schemaTestColumns(), output: &output, t: t}
	writer := &dataWriter{ctx: context.Background(), client: buffer.NewWriter(&output)}
	if err := (&Server{}).writeSQLResultStream(stream, writer, nil); err != nil {
		t.Fatal(err)
	}
}
