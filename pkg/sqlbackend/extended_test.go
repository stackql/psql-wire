package sqlbackend

import (
	"context"
	"io"
	"testing"

	"github.com/stackql/psql-wire/pkg/sqldata"
)

func TestNewDefaultExtendedQueryBackend(t *testing.T) {
	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		return nil, nil
	}
	backend := NewSimpleSQLBackend(cb)
	ext := NewDefaultExtendedQueryBackend(backend)
	if ext == nil {
		t.Fatal("expected non-nil extended backend")
	}
}

func TestDefaultExtendedQueryBackend_HandleParse(t *testing.T) {
	tests := []struct {
		name      string
		stmtName  string
		query     string
		paramOIDs []uint32
	}{
		{
			name:      "no params",
			stmtName:  "",
			query:     "SELECT 1",
			paramOIDs: []uint32{},
		},
		{
			name:      "named statement with params",
			stmtName:  "my_stmt",
			query:     "SELECT $1, $2",
			paramOIDs: []uint32{25, 23},
		},
		{
			name:      "nil params",
			stmtName:  "",
			query:     "SELECT 1",
			paramOIDs: nil,
		},
	}

	ext := newTestExtendedBackend(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ext.HandleParse(context.Background(), tt.stmtName, tt.query, tt.paramOIDs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.paramOIDs) {
				t.Fatalf("got %d OIDs, want %d", len(got), len(tt.paramOIDs))
			}
			for i := range got {
				if got[i] != tt.paramOIDs[i] {
					t.Errorf("OID[%d] = %d, want %d", i, got[i], tt.paramOIDs[i])
				}
			}
		})
	}
}

func TestDefaultExtendedQueryBackend_HandleBind(t *testing.T) {
	ext := newTestExtendedBackend(t)

	err := ext.HandleBind(
		context.Background(),
		"",          // portal
		"",          // statement
		[]int16{0},  // param formats (text)
		[][]byte{[]byte("hello")},
		[]int16{0},  // result formats (text)
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDefaultExtendedQueryBackend_HandleDescribeStatement(t *testing.T) {
	ext := newTestExtendedBackend(t)

	paramOIDs := []uint32{25, 23}
	gotOIDs, gotCols, err := ext.HandleDescribeStatement(
		context.Background(),
		"my_stmt",
		"SELECT $1, $2",
		paramOIDs,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(gotOIDs) != len(paramOIDs) {
		t.Fatalf("got %d OIDs, want %d", len(gotOIDs), len(paramOIDs))
	}
	if gotCols != nil {
		t.Fatalf("expected nil columns from default backend, got %v", gotCols)
	}
}

func TestDefaultExtendedQueryBackend_HandleDescribePortal(t *testing.T) {
	ext := newTestExtendedBackend(t)

	cols, err := ext.HandleDescribePortal(
		context.Background(),
		"",
		"",
		"SELECT 1",
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cols != nil {
		t.Fatalf("expected nil columns from default backend, got %v", cols)
	}
}

func TestDefaultExtendedQueryBackend_HandleExecute(t *testing.T) {
	row := sqldata.NewSQLRow([]interface{}{"Alice", 30})
	col := sqldata.NewSQLColumn(sqldata.NewSQLTable(0, ""), "name", 0, 25, 256, -1, "text")
	result := sqldata.NewSQLResult([]sqldata.ISQLColumn{col}, 0, 0, []sqldata.ISQLRow{row})
	stream := sqldata.NewSimpleSQLResultStream(result)

	var receivedQuery string
	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		receivedQuery = query
		return stream, nil
	}

	backend := NewSimpleSQLBackend(cb)
	ext := NewDefaultExtendedQueryBackend(backend)

	got, err := ext.HandleExecute(
		context.Background(),
		"",                    // portal
		"",                    // statement
		"SELECT name FROM t",  // query
		nil,                   // param formats
		nil,                   // param values
		nil,                   // result formats
		0,                     // max rows (unlimited)
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedQuery != "SELECT name FROM t" {
		t.Fatalf("got query %q, want %q", receivedQuery, "SELECT name FROM t")
	}

	res, err := got.Read()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	rows := res.GetRows()
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	data := rows[0].GetRowDataNaive()
	if data[0] != "Alice" {
		t.Errorf("got %v, want Alice", data[0])
	}
}

func TestDefaultExtendedQueryBackend_HandleCloseStatement(t *testing.T) {
	ext := newTestExtendedBackend(t)
	if err := ext.HandleCloseStatement(context.Background(), "my_stmt"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDefaultExtendedQueryBackend_HandleClosePortal(t *testing.T) {
	ext := newTestExtendedBackend(t)
	if err := ext.HandleClosePortal(context.Background(), "my_portal"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// newTestExtendedBackend creates a DefaultExtendedQueryBackend with a no-op simple backend.
func newTestExtendedBackend(t *testing.T) IExtendedQueryBackend {
	t.Helper()
	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		return nil, nil
	}
	return NewDefaultExtendedQueryBackend(NewSimpleSQLBackend(cb))
}
