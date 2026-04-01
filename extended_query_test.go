package wire

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stackql/psql-wire/internal/mock"
	"github.com/stackql/psql-wire/internal/types"
	"github.com/stackql/psql-wire/pkg/sqlbackend"
	"github.com/stackql/psql-wire/pkg/sqldata"
)

// --- mock client helpers for extended query protocol ---

// sendParse sends a Parse message: statement name, query, and parameter OIDs.
func sendParse(t *testing.T, client *mock.Client, stmtName, query string, paramOIDs []uint32) {
	t.Helper()
	client.Start(types.ClientParse)
	client.AddString(stmtName)
	client.AddNullTerminate()
	client.AddString(query)
	client.AddNullTerminate()
	client.AddInt16(int16(len(paramOIDs)))
	for _, oid := range paramOIDs {
		client.AddInt32(int32(oid))
	}
	if err := client.End(); err != nil {
		t.Fatalf("sendParse: %v", err)
	}
}

// sendBind sends a Bind message with no parameters and default (text) formats.
func sendBind(t *testing.T, client *mock.Client, portalName, stmtName string) {
	t.Helper()
	client.Start(types.ClientBind)
	client.AddString(portalName)
	client.AddNullTerminate()
	client.AddString(stmtName)
	client.AddNullTerminate()
	client.AddInt16(0) // no param format codes
	client.AddInt16(0) // no param values
	client.AddInt16(0) // no result format codes
	if err := client.End(); err != nil {
		t.Fatalf("sendBind: %v", err)
	}
}

// sendDescribeStatement sends a Describe message for a statement.
func sendDescribeStatement(t *testing.T, client *mock.Client, stmtName string) {
	t.Helper()
	client.Start(types.ClientDescribe)
	client.AddByte('S')
	client.AddString(stmtName)
	client.AddNullTerminate()
	if err := client.End(); err != nil {
		t.Fatalf("sendDescribeStatement: %v", err)
	}
}

// sendDescribePortal sends a Describe message for a portal.
func sendDescribePortal(t *testing.T, client *mock.Client, portalName string) {
	t.Helper()
	client.Start(types.ClientDescribe)
	client.AddByte('P')
	client.AddString(portalName)
	client.AddNullTerminate()
	if err := client.End(); err != nil {
		t.Fatalf("sendDescribePortal: %v", err)
	}
}

// sendExecute sends an Execute message.
func sendExecute(t *testing.T, client *mock.Client, portalName string, maxRows int32) {
	t.Helper()
	client.Start(types.ClientExecute)
	client.AddString(portalName)
	client.AddNullTerminate()
	client.AddInt32(maxRows)
	if err := client.End(); err != nil {
		t.Fatalf("sendExecute: %v", err)
	}
}

// sendSync sends a Sync message.
func sendSync(t *testing.T, client *mock.Client) {
	t.Helper()
	client.Start(types.ClientSync)
	if err := client.End(); err != nil {
		t.Fatalf("sendSync: %v", err)
	}
}

// sendCloseStatement sends a Close message for a statement.
func sendCloseStatement(t *testing.T, client *mock.Client, stmtName string) {
	t.Helper()
	client.Start(types.ClientClose)
	client.AddByte('S')
	client.AddString(stmtName)
	client.AddNullTerminate()
	if err := client.End(); err != nil {
		t.Fatalf("sendCloseStatement: %v", err)
	}
}

// sendClosePortal sends a Close message for a portal.
func sendClosePortal(t *testing.T, client *mock.Client, portalName string) {
	t.Helper()
	client.Start(types.ClientClose)
	client.AddByte('P')
	client.AddString(portalName)
	client.AddNullTerminate()
	if err := client.End(); err != nil {
		t.Fatalf("sendClosePortal: %v", err)
	}
}

// expectMsg reads the next message and asserts its type matches.
func expectMsg(t *testing.T, client *mock.Client, expected types.ServerMessage) {
	t.Helper()
	got, _, err := client.ReadTypedMsg()
	if err != nil {
		t.Fatalf("expectMsg(%c): read error: %v", expected, err)
	}
	if got != expected {
		t.Fatalf("expected message type %c (%d), got %c (%d)", expected, expected, got, got)
	}
}

// expectReadyForQuery reads ReadyForQuery and asserts the transaction status byte.
func expectReadyForQuery(t *testing.T, client *mock.Client, expectedStatus types.ServerStatus) {
	t.Helper()
	expectMsg(t, client, types.ServerReady)
	bb, err := client.GetBytes(1)
	if err != nil {
		t.Fatalf("expectReadyForQuery: %v", err)
	}
	if types.ServerStatus(bb[0]) != expectedStatus {
		t.Fatalf("expected ReadyForQuery status %c, got %c", expectedStatus, bb[0])
	}
}

// --- test server helpers ---

// newTestServer creates a server with a simple SQL backend that returns a fixed
// result for any query.
func newTestServer(t *testing.T) (*Server, *net.TCPAddr) {
	t.Helper()
	cols := []sqldata.ISQLColumn{
		sqldata.NewSQLColumn(sqldata.NewSQLTable(0, ""), "id", 0, 23, 4, -1, "text"),
	}
	rows := []sqldata.ISQLRow{
		sqldata.NewSQLRow([]interface{}{1}),
	}
	result := sqldata.NewSQLResult(cols, 0, 0, rows)
	stream := sqldata.NewSimpleSQLResultStream(result)

	qcb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		return stream, nil
	}

	factory := sqlbackend.NewSimpleSQLBackendFactory(qcb)
	server, err := NewServer(SQLBackendFactory(factory))
	if err != nil {
		t.Fatal(err)
	}
	address := TListenAndServe(t, server)
	return server, address
}

// connectAndHandshake dials the server and does a full handshake up to ReadyForQuery.
func connectAndHandshake(t *testing.T, address *net.TCPAddr) *mock.Client {
	t.Helper()
	conn, err := net.Dial("tcp", address.String())
	if err != nil {
		t.Fatal(err)
	}
	client := mock.NewClient(conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)
	return client
}

// --- tests ---

func TestExtendedQueryHappyPath(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Parse → ParseComplete
	sendParse(t, client, "", "SELECT 1", nil)
	expectMsg(t, client, types.ServerParseComplete)

	// Bind → BindComplete
	sendBind(t, client, "", "")
	expectMsg(t, client, types.ServerBindComplete)

	// Describe statement → ParameterDescription + NoData
	sendDescribeStatement(t, client, "")
	expectMsg(t, client, types.ServerParameterDescription)
	expectMsg(t, client, types.ServerNoData)

	// Execute → RowDescription + DataRow(s) + CommandComplete (result via default extended backend)
	sendExecute(t, client, "", 0)
	// The default backend returns a result with one row
	expectMsg(t, client, types.ServerRowDescription)
	expectMsg(t, client, types.ServerDataRow)
	expectMsg(t, client, types.ServerCommandComplete)

	// Sync → ReadyForQuery(Idle)
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerIdle)

	client.Close(t)
}

func TestExtendedQueryParseError_DiscardsThroughSync(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Bind to a non-existent statement → should trigger error state
	sendBind(t, client, "", "nonexistent_stmt")
	expectMsg(t, client, types.ServerErrorResponse)

	// Send more messages — these should all be discarded
	sendDescribeStatement(t, client, "")
	sendExecute(t, client, "", 0)

	// Sync should recover and return ReadyForQuery with error status
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerTransactionFailed)

	// After recovery, normal operations should work again.
	// Send a new Parse → should succeed.
	sendParse(t, client, "", "SELECT 1", nil)
	expectMsg(t, client, types.ServerParseComplete)

	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerIdle)

	client.Close(t)
}

func TestExtendedQueryExecuteError_DiscardsThroughSync(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Execute a non-existent portal → error
	sendExecute(t, client, "no_such_portal", 0)
	expectMsg(t, client, types.ServerErrorResponse)

	// Another Execute should be discarded
	sendExecute(t, client, "no_such_portal", 0)

	// Sync recovers
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerTransactionFailed)

	client.Close(t)
}

func TestExtendedQueryDescribeError_DiscardsThroughSync(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Describe a non-existent statement → error
	sendDescribeStatement(t, client, "no_such_stmt")
	expectMsg(t, client, types.ServerErrorResponse)

	// Bind should be discarded
	sendBind(t, client, "", "")

	// Sync recovers
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerTransactionFailed)

	client.Close(t)
}

func TestExtendedQueryCloseStatement(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Parse a named statement
	sendParse(t, client, "my_stmt", "SELECT 1", nil)
	expectMsg(t, client, types.ServerParseComplete)

	// Close statement → CloseComplete
	sendCloseStatement(t, client, "my_stmt")
	expectMsg(t, client, types.ServerCloseComplete)

	// Sync
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerIdle)

	// Statement no longer exists → Bind to it should error
	sendBind(t, client, "", "my_stmt")
	expectMsg(t, client, types.ServerErrorResponse)

	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerTransactionFailed)

	client.Close(t)
}

func TestExtendedQueryClosePortal(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Parse + Bind to create a portal
	sendParse(t, client, "", "SELECT 1", nil)
	expectMsg(t, client, types.ServerParseComplete)

	sendBind(t, client, "my_portal", "")
	expectMsg(t, client, types.ServerBindComplete)

	// Close portal → CloseComplete
	sendClosePortal(t, client, "my_portal")
	expectMsg(t, client, types.ServerCloseComplete)

	// Sync
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerIdle)

	// Portal no longer exists → Execute should error
	sendExecute(t, client, "my_portal", 0)
	expectMsg(t, client, types.ServerErrorResponse)

	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerTransactionFailed)

	client.Close(t)
}

func TestExtendedQueryMultipleSyncsAfterError(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Trigger error state
	sendExecute(t, client, "nonexistent", 0)
	expectMsg(t, client, types.ServerErrorResponse)

	// First Sync → ReadyForQuery(E)
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerTransactionFailed)

	// Second Sync (no error state) → ReadyForQuery(I)
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerIdle)

	client.Close(t)
}

func TestExtendedQueryNamedStatements(t *testing.T) {
	t.Parallel()
	_, address := newTestServer(t)
	client := connectAndHandshake(t, address)

	// Parse two named statements
	sendParse(t, client, "stmt_a", "SELECT 1", nil)
	expectMsg(t, client, types.ServerParseComplete)

	sendParse(t, client, "stmt_b", "SELECT 2", nil)
	expectMsg(t, client, types.ServerParseComplete)

	// Bind and execute stmt_b
	sendBind(t, client, "", "stmt_b")
	expectMsg(t, client, types.ServerBindComplete)

	sendExecute(t, client, "", 0)
	expectMsg(t, client, types.ServerRowDescription)
	expectMsg(t, client, types.ServerDataRow)
	expectMsg(t, client, types.ServerCommandComplete)

	// Bind and execute stmt_a
	sendBind(t, client, "", "stmt_a")
	expectMsg(t, client, types.ServerBindComplete)

	sendExecute(t, client, "", 0)
	expectMsg(t, client, types.ServerRowDescription)
	expectMsg(t, client, types.ServerDataRow)
	expectMsg(t, client, types.ServerCommandComplete)

	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerIdle)

	client.Close(t)
}

func TestSimpleQueryAfterExtendedQueryError(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string, writer DataWriter) error {
		return writer.Complete("", "OK")
	}

	server, err := NewServer(SimpleQuery(handler))
	if err != nil {
		t.Fatal(err)
	}
	address := TListenAndServe(t, server)
	client := connectAndHandshake(t, address)

	// Trigger extended query error (no backend, so Bind to unnamed stmt fails)
	sendParse(t, client, "", "SELECT 1", nil)
	expectMsg(t, client, types.ServerParseComplete)

	// Execute unnamed portal which doesn't exist → error
	sendExecute(t, client, "", 0)
	expectMsg(t, client, types.ServerErrorResponse)

	// Sync to recover
	sendSync(t, client)
	expectReadyForQuery(t, client, types.ServerTransactionFailed)

	// Now a simple query should work fine
	client.Start(types.ClientSimpleQuery)
	client.AddString(fmt.Sprintf("SELECT 1"))
	client.AddNullTerminate()
	if err := client.End(); err != nil {
		t.Fatal(err)
	}

	expectMsg(t, client, types.ServerCommandComplete)
	expectReadyForQuery(t, client, types.ServerIdle)

	client.Close(t)
}
