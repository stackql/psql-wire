package wire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"
	"github.com/lib/pq/oid"
	"github.com/sirupsen/logrus"
	"github.com/stackql/psql-wire/internal/types"
	"github.com/stackql/psql-wire/pkg/sqlbackend"
	"github.com/stackql/psql-wire/pkg/sqldata"
)

const fuzzMessageBufferSize = 256

var fuzzLogger = func() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logger
}()

func FuzzStartup(f *testing.F) {
	f.Add(append([]byte{0}, fuzzStartupPacket(uint32(types.Version30))...))
	f.Add(append([]byte{0}, fuzzStartupPacket(uint32(types.VersionSSLRequest))...))
	f.Add(append([]byte{2}, fuzzStartupPacket(uint32(types.Version30))...))
	f.Add(append([]byte{2}, fuzzStartupPacket(uint32(types.VersionSSLRequest))...))
	f.Add(append([]byte{1}, append(fuzzStartupPacket(uint32(types.Version30)), fuzzTypedMessage(byte(types.ClientPassword), []byte("pw\x00"))...)...))
	f.Add(append([]byte{0}, fuzzStartupPacket(uint32(types.VersionCancel))...))

	f.Fuzz(func(t *testing.T, input []byte) {
		mode, startup := byte(0), input
		if len(input) > 0 {
			mode, startup = input[0]%3, input[1:]
		}

		srv := newFuzzServer(nil)
		switch mode {
		case 1:
			srv.Auth = ClearTextPassword(func(_, password string) (bool, error) {
				return password == "pw", nil
			})
		case 2:
			srv.ClientAuth = tls.RequireAndVerifyClientCert
		}
		runFuzzServer(t, srv, startup, nil)
	})
}

func FuzzSessionRaw(f *testing.F) {
	f.Add(fuzzTypedMessage(byte(types.ClientSimpleQuery), []byte("select 1\x00")))
	f.Add(append(
		fuzzTypedMessage(byte(types.ClientSimpleQuery), []byte("select 1\x00")),
		fuzzTypedMessage(byte(types.ClientTerminate), nil)...,
	))
	f.Add(fuzzTypedMessage(byte(types.ClientSync), nil))
	f.Add([]byte{byte(types.ClientSimpleQuery), 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, commands []byte) {
		input := append(fuzzStartupPacket(uint32(types.Version30)), commands...)
		srv := newFuzzServer(newFuzzBackendFactory(2, false))
		runFuzzServer(t, srv, input, nil)
	})
}

func FuzzSessionStructured(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 5})
	f.Add([]byte{2, 7, 0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{6, 0, 1, 3, 5})
	f.Add([]byte{6, 7})
	f.Add([]byte{1, 0, 1, 2, 3, 5})

	f.Fuzz(func(t *testing.T, script []byte) {
		mode := byte(0)
		if len(script) > 0 {
			mode = script[0] % 7
			script = script[1:]
		}
		commands, requests := fuzzStructuredMessages(script)
		srv := newFuzzServer(newFuzzBackendFactory(mode, mode == 6))
		messages := runFuzzServer(t, srv, append(fuzzStartupPacket(uint32(types.Version30)), commands...), requests)
		verifyStructuredResponses(t, requests, messages)
	})
}

func newFuzzServer(factory sqlbackend.SQLBackendFactory) *Server {
	options := []OptionFn{
		MessageBufferSize(fuzzMessageBufferSize),
		Logger(fuzzLogger),
	}
	if factory != nil {
		options = append(options, SQLBackendFactory(factory))
	}
	srv, err := NewServer(options...)
	if err != nil {
		panic(err)
	}
	return srv
}

type fuzzMemoryConn struct {
	input     *bytes.Reader
	output    bytes.Buffer
	maxOutput int
}

func newFuzzMemoryConn(input []byte) *fuzzMemoryConn {
	maxOutput := len(input)*16 + 64*1024
	return &fuzzMemoryConn{
		input:     bytes.NewReader(input),
		maxOutput: maxOutput,
	}
}

func (c *fuzzMemoryConn) Read(p []byte) (int, error) { return c.input.Read(p) }

func (c *fuzzMemoryConn) Write(p []byte) (int, error) {
	if len(p) > c.maxOutput-c.output.Len() {
		return 0, errors.New("fuzz output exceeded its input-relative memory budget")
	}
	return c.output.Write(p)
}

func (c *fuzzMemoryConn) Close() error                     { return nil }
func (c *fuzzMemoryConn) LocalAddr() net.Addr              { return fuzzAddr("local") }
func (c *fuzzMemoryConn) RemoteAddr() net.Addr             { return fuzzAddr("remote") }
func (c *fuzzMemoryConn) SetDeadline(time.Time) error      { return nil }
func (c *fuzzMemoryConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fuzzMemoryConn) SetWriteDeadline(time.Time) error { return nil }

type fuzzAddr string

func (a fuzzAddr) Network() string { return string(a) }
func (a fuzzAddr) String() string  { return string(a) }

func runFuzzServer(t *testing.T, srv *Server, input []byte, requests []byte) []pgproto3.BackendMessage {
	t.Helper()
	conn := newFuzzMemoryConn(input)
	before := runtime.MemStats{}
	runtime.ReadMemStats(&before)
	started := time.Now()
	_ = srv.serve(context.Background(), conn)
	elapsed := time.Since(started)
	after := runtime.MemStats{}
	runtime.ReadMemStats(&after)

	if elapsed > time.Second {
		t.Fatalf("serving an exhausted in-memory input took %s", elapsed)
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	allocationLimit := uint64(len(input))*512 + 2*1024*1024
	if allocated > allocationLimit {
		t.Fatalf("serving %d input bytes allocated %d bytes (limit %d)", len(input), allocated, allocationLimit)
	}

	messages := decodeFuzzBackendMessages(t, conn.output.Bytes())
	if requests != nil && len(messages) > 0 {
		readyCount := 0
		for _, message := range messages {
			if _, ok := message.(*pgproto3.ReadyForQuery); ok {
				readyCount++
			}
		}
		maxReady := 1
		for _, request := range requests {
			if request == byte(types.ClientSimpleQuery) || request == byte(types.ClientSync) {
				maxReady++
			}
		}
		if readyCount == 0 || readyCount > maxReady {
			t.Fatalf("ReadyForQuery count %d is outside [1, %d]", readyCount, maxReady)
		}
	}
	return messages
}

func decodeFuzzBackendMessages(t *testing.T, output []byte) []pgproto3.BackendMessage {
	t.Helper()
	// PostgreSQL 16 Message Formats frame backend responses independently from
	// this library's writer; pgproto3 decodes every complete response:
	// https://www.postgresql.org/docs/16/protocol-message-formats.html
	// The same section specifies the unframed 'S' or 'N' SSLRequest response.
	if len(output) > 0 && output[0] == 'N' && (len(output) == 1 || output[1] == 'R') {
		output = output[1:]
	}
	chunks := &fuzzChunkReader{reader: bytes.NewReader(output), remaining: len(output)}
	decoder := pgproto3.NewFrontend(chunks, io.Discard)
	var messages []pgproto3.BackendMessage
	for chunks.remaining > 0 {
		message, err := decoder.Receive()
		if err != nil {
			t.Fatalf("server output is not a sequence of complete backend messages: %v", err)
		}
		messages = append(messages, message)
	}
	return messages
}

type fuzzChunkReader struct {
	reader    *bytes.Reader
	remaining int
}

func (r *fuzzChunkReader) Next(n int) ([]byte, error) {
	if n < 0 || n > r.remaining {
		return nil, io.ErrUnexpectedEOF
	}
	chunk := make([]byte, n)
	if _, err := io.ReadFull(r.reader, chunk); err != nil {
		return nil, err
	}
	r.remaining -= n
	return chunk, nil
}

func fuzzStartupPacket(version uint32) []byte {
	body := make([]byte, 4, 64)
	binary.BigEndian.PutUint32(body, version)
	body = append(body, []byte("user\x00fuzzer\x00database\x00fuzz\x00\x00")...)
	packet := make([]byte, 4, len(body)+4)
	binary.BigEndian.PutUint32(packet, uint32(len(body)+4))
	return append(packet, body...)
}

func fuzzTypedMessage(kind byte, body []byte) []byte {
	message := make([]byte, 5, len(body)+5)
	message[0] = kind
	binary.BigEndian.PutUint32(message[1:5], uint32(len(body)+4))
	return append(message, body...)
}

func fuzzCString(dst []byte, value string) []byte {
	dst = append(dst, value...)
	return append(dst, 0)
}

func fuzzStructuredMessages(script []byte) ([]byte, []byte) {
	var commands []byte
	var requests []byte
	for _, op := range script {
		var kind byte
		var body []byte
		switch op % 9 {
		case 0:
			kind = byte(types.ClientParse)
			body = fuzzCString(body, "s")
			body = fuzzCString(body, fmt.Sprintf("select %d", op))
			body = append(body, 0, 0)
		case 1:
			kind = byte(types.ClientBind)
			body = fuzzCString(body, "p")
			body = fuzzCString(body, "s")
			body = append(body, 0, 0, 0, 0, 0, 0)
		case 2:
			kind = byte(types.ClientDescribe)
			if op&0x10 == 0 {
				body = append(body, byte('S'))
				body = fuzzCString(body, "s")
			} else {
				body = append(body, byte('P'))
				body = fuzzCString(body, "p")
			}
		case 3:
			kind = byte(types.ClientExecute)
			body = fuzzCString(body, "p")
			var maxRows [4]byte
			binary.BigEndian.PutUint32(maxRows[:], uint32(op))
			body = append(body, maxRows[:]...)
		case 4:
			kind = byte(types.ClientClose)
			if op&0x10 == 0 {
				body = append(body, byte('S'))
				body = fuzzCString(body, "s")
			} else {
				body = append(body, byte('P'))
				body = fuzzCString(body, "p")
			}
		case 5:
			kind = byte(types.ClientSync)
		case 6:
			kind = byte(types.ClientFlush)
		case 7:
			kind = byte(types.ClientSimpleQuery)
			body = fuzzCString(body, "select "+strconv.Itoa(int(op)))
		case 8:
			kind = byte(types.ClientTerminate)
		}
		commands = append(commands, fuzzTypedMessage(kind, body)...)
		requests = append(requests, kind)
		if kind == byte(types.ClientTerminate) {
			break
		}
	}
	return commands, requests
}

func verifyStructuredResponses(t *testing.T, requests []byte, messages []pgproto3.BackendMessage) {
	t.Helper()
	if len(messages) == 0 {
		t.Fatal("completed structured startup produced no backend messages")
	}
	// PostgreSQL 16 Protocol Flow specifies the initial ReadyForQuery before
	// the first query cycle: https://www.postgresql.org/docs/16/protocol-flow.html
	initialReady := -1
	for i, message := range messages {
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			initialReady = i
			break
		}
	}
	if initialReady < 0 {
		t.Fatal("completed structured startup did not send ReadyForQuery")
	}
	for i := 0; i < initialReady; i++ {
		if _, ok := messages[i].(*pgproto3.ReadyForQuery); ok {
			t.Fatal("startup emitted more than one initial ReadyForQuery")
		}
	}

	// PostgreSQL 16 Protocol Flow specifies that an extended-protocol error
	// suppresses responses until Sync; a simple-query error is followed by
	// ReadyForQuery: https://www.postgresql.org/docs/16/protocol-flow.html
	for i, message := range messages {
		if _, ok := message.(*pgproto3.ErrorResponse); ok && i+1 < len(messages) {
			if _, ready := messages[i+1].(*pgproto3.ReadyForQuery); !ready {
				t.Fatal("server emitted a response after ErrorResponse before Sync")
			}
		}
	}

	// PostgreSQL 16 Protocol Flow specifies one ReadyForQuery for each processed
	// Query and Sync; Terminate is not followed by a backend response:
	// https://www.postgresql.org/docs/16/protocol-flow.html
	verifyReadyResponseGroups(t, requests, messages[initialReady+1:])
}

func verifyReadyResponseGroups(t *testing.T, requests []byte, messages []pgproto3.BackendMessage) {
	t.Helper()
	cursor := 0
	inErrorState := false
	for _, request := range requests {
		if request == byte(types.ClientTerminate) {
			break
		}
		if inErrorState {
			if request == byte(types.ClientSync) {
				consumeFuzzResponse(t, messages, &cursor, func(message pgproto3.BackendMessage) bool {
					_, ok := message.(*pgproto3.ReadyForQuery)
					return ok
				}, "Sync ReadyForQuery")
				inErrorState = false
			}
			continue
		}

		switch types.ClientMessage(request) {
		case types.ClientSimpleQuery, types.ClientSync:
			consumeReadyForQuery(t, messages, &cursor)
		case types.ClientParse:
			inErrorState = consumeFuzzResponse(t, messages, &cursor, isParseCompleteOrError, "ParseComplete or ErrorResponse")
		case types.ClientBind:
			inErrorState = consumeFuzzResponse(t, messages, &cursor, isBindCompleteOrError, "BindComplete or ErrorResponse")
		case types.ClientDescribe:
			inErrorState = consumeFuzzResponse(t, messages, &cursor, isDescribeCompleteOrError, "Describe response")
		case types.ClientExecute:
			inErrorState = consumeFuzzResponse(t, messages, &cursor, isExecuteCompleteOrError, "Execute response")
		case types.ClientClose:
			inErrorState = consumeFuzzResponse(t, messages, &cursor, isCloseCompleteOrError, "CloseComplete or ErrorResponse")
		case types.ClientFlush:
		default:
			t.Fatalf("unexpected structured request type %q", request)
		}
	}
	if cursor != len(messages) {
		t.Fatalf("unexpected trailing backend responses: consumed %d of %d", cursor, len(messages))
	}
}

func consumeReadyForQuery(t *testing.T, messages []pgproto3.BackendMessage, cursor *int) {
	t.Helper()
	for *cursor < len(messages) {
		message := messages[*cursor]
		*cursor++
		if _, ok := message.(*pgproto3.ReadyForQuery); ok {
			return
		}
	}
	t.Fatal("missing ReadyForQuery")
}

func consumeFuzzResponse(t *testing.T, messages []pgproto3.BackendMessage, cursor *int, done func(pgproto3.BackendMessage) bool, description string) bool {
	t.Helper()
	for *cursor < len(messages) {
		message := messages[*cursor]
		*cursor++
		if _, failed := message.(*pgproto3.ErrorResponse); failed {
			return true
		}
		if done(message) {
			return false
		}
	}
	t.Fatalf("missing %s in server output", description)
	return false
}

func isParseCompleteOrError(message pgproto3.BackendMessage) bool {
	_, ok := message.(*pgproto3.ParseComplete)
	return ok
}

func isBindCompleteOrError(message pgproto3.BackendMessage) bool {
	_, ok := message.(*pgproto3.BindComplete)
	return ok
}

func isDescribeCompleteOrError(message pgproto3.BackendMessage) bool {
	switch message.(type) {
	case *pgproto3.NoData, *pgproto3.RowDescription:
		return true
	default:
		return false
	}
}

func isExecuteCompleteOrError(message pgproto3.BackendMessage) bool {
	_, ok := message.(*pgproto3.CommandComplete)
	return ok
}

func isCloseCompleteOrError(message pgproto3.BackendMessage) bool {
	_, ok := message.(*pgproto3.CloseComplete)
	return ok
}

type fuzzBackendFactory struct {
	mode byte
	fail bool
}

func newFuzzBackendFactory(mode byte, fail bool) sqlbackend.SQLBackendFactory {
	return fuzzBackendFactory{mode: mode, fail: fail}
}

func (f fuzzBackendFactory) NewSQLBackend() (sqlbackend.ISQLBackend, error) {
	if f.fail {
		return nil, errors.New("fuzz backend factory failure")
	}
	return &fuzzBackend{mode: f.mode}, nil
}

type fuzzBackend struct {
	mode byte
}

func (b *fuzzBackend) HandleSimpleQuery(context.Context, string) (sqldata.ISQLResultStream, error) {
	return b.stream()
}

func (b *fuzzBackend) SplitCompoundQuery(query string) ([]string, error) {
	return sqlbackend.NewSimpleSQLBackend(nil).SplitCompoundQuery(query)
}

func (b *fuzzBackend) GetDebugStr() string { return "" }

func (b *fuzzBackend) HandleParse(_ context.Context, _ string, _ string, oids []uint32) ([]uint32, error) {
	if b.mode == 1 {
		return nil, errors.New("fuzz parse failure")
	}
	return oids, nil
}

func (b *fuzzBackend) HandleBind(context.Context, string, string, []int16, [][]byte, []int16) error {
	if b.mode == 1 {
		return errors.New("fuzz bind failure")
	}
	return nil
}

func (b *fuzzBackend) HandleDescribeStatement(_ context.Context, _ string, _ string, oids []uint32) ([]uint32, []sqldata.ISQLColumn, error) {
	if b.mode == 1 {
		return nil, nil, errors.New("fuzz describe failure")
	}
	return oids, nil, nil
}

func (b *fuzzBackend) HandleDescribePortal(context.Context, string, string, string, []uint32) ([]sqldata.ISQLColumn, error) {
	if b.mode == 1 {
		return nil, errors.New("fuzz describe failure")
	}
	return nil, nil
}

func (b *fuzzBackend) HandleExecute(context.Context, string, string, string, []int16, [][]byte, []int16, int32) (sqldata.ISQLResultStream, error) {
	return b.stream()
}

func (b *fuzzBackend) HandleCloseStatement(context.Context, string) error { return nil }
func (b *fuzzBackend) HandleClosePortal(context.Context, string) error    { return nil }

func (b *fuzzBackend) stream() (sqldata.ISQLResultStream, error) {
	if b.mode == 1 {
		return nil, errors.New("fuzz query failure")
	}
	if b.mode == 2 {
		return nil, nil
	}

	columnCount := 1
	oidValue := uint32(oid.T_text)
	rowValues := []interface{}{"fuzz"}
	if b.mode == 3 {
		rowValues = []interface{}{"fuzz", "extra"}
	} else if b.mode == 4 {
		oidValue = uint32(oid.T_int4)
		rowValues = []interface{}{struct{}{}}
	}
	columns := make([]sqldata.ISQLColumn, columnCount)
	for i := range columns {
		columns[i] = sqldata.NewSQLColumn(sqldata.NewSQLTable(0, ""), "value", int16(i+1), oidValue, -1, -1, "TextFormat")
	}
	result := sqldata.NewSQLResult(columns, 0, 0, []sqldata.ISQLRow{
		sqldata.NewSQLRow(rowValues),
	})
	return &fuzzResultStream{result: result, failAfterFirst: b.mode == 5}, nil
}

type fuzzResultStream struct {
	result         sqldata.ISQLResult
	failAfterFirst bool
	read           bool
}

func (s *fuzzResultStream) GetColumns() []sqldata.ISQLColumn { return s.result.GetColumns() }

func (s *fuzzResultStream) Read() (sqldata.ISQLResult, error) {
	if !s.read {
		s.read = true
		if s.failAfterFirst {
			return s.result, nil
		}
		return s.result, io.EOF
	}
	if s.failAfterFirst {
		return nil, errors.New("fuzz stream failure")
	}
	return nil, io.EOF
}

func (*fuzzResultStream) Write(sqldata.ISQLResult) error { return errors.New("write unsupported") }
func (*fuzzResultStream) Close() error                   { return nil }

type fuzzRecordingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []*fuzzRecordingConn
}

func (l *fuzzRecordingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	recording := &fuzzRecordingConn{Conn: conn, done: make(chan struct{})}
	l.mu.Lock()
	l.conns = append(l.conns, recording)
	l.mu.Unlock()
	return recording, nil
}

type fuzzRecordingConn struct {
	net.Conn
	mu       sync.Mutex
	received bytes.Buffer
	done     chan struct{}
	close    sync.Once
}

func (c *fuzzRecordingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.mu.Lock()
		_, _ = c.received.Write(p[:n])
		c.mu.Unlock()
	}
	return n, err
}

func (c *fuzzRecordingConn) Close() error {
	err := c.Conn.Close()
	c.close.Do(func() {
		close(c.done)
	})
	return err
}

func (l *fuzzRecordingListener) recordSeeds(t *testing.T) {
	t.Helper()
	if os.Getenv("PSQL_WIRE_RECORD_FUZZ_SEEDS") != "1" {
		return
	}
	l.mu.Lock()
	conns := append([]*fuzzRecordingConn(nil), l.conns...)
	l.mu.Unlock()

	for index, conn := range conns {
		select {
		case <-conn.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for client connection %d to finish", index)
		}

		conn.mu.Lock()
		clientBytes := append([]byte(nil), conn.received.Bytes()...)
		conn.mu.Unlock()
		recordFuzzClientSession(t, clientBytes)
	}
}

func recordFuzzClientSession(t *testing.T, clientBytes []byte) {
	t.Helper()
	if os.Getenv("PSQL_WIRE_RECORD_FUZZ_SEEDS") != "1" {
		return
	}
	if len(clientBytes) < 8 {
		return
	}

	offset := 0
	var sslRequest []byte
	firstLength := int(binary.BigEndian.Uint32(clientBytes[:4]))
	if firstLength == 8 {
		version := binary.BigEndian.Uint32(clientBytes[4:8])
		if version == uint32(types.VersionSSLRequest) {
			sslRequest = clientBytes[:8]
			offset = 8
		} else if version != uint32(types.Version30) {
			return
		}
	}
	if len(clientBytes)-offset < 8 {
		return
	}
	startupLength := int(binary.BigEndian.Uint32(clientBytes[offset : offset+4]))
	if startupLength < 8 || startupLength > len(clientBytes)-offset {
		return
	}

	startupSeed := append([]byte{0}, sslRequest...)
	startupSeed = append(startupSeed, fuzzStartupPacket(uint32(types.Version30))...)
	writeFuzzCorpusSeed(t, "FuzzStartup", startupSeed)

	commands := clientBytes[offset+startupLength:]
	if len(commands) != 0 {
		writeFuzzCorpusSeed(t, "FuzzSessionRaw", commands)
	}
}

func writeFuzzCorpusSeed(t *testing.T, target string, input []byte) {
	t.Helper()
	digest := sha256.Sum256(input)
	name := fmt.Sprintf("recorded-%x", digest[:8])
	path := filepath.Join("testdata", "fuzz", target, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	content := []byte("go test fuzz v1\n[]byte(" + strconv.QuoteToASCII(string(input)) + ")\n")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}
