package sqlbackend

import (
	"context"
	"io"
	"testing"

	"github.com/stackql/psql-wire/pkg/sqldata"
)

func TestNewSimpleSQLBackend(t *testing.T) {
	called := false
	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		called = true
		return nil, nil
	}

	backend := NewSimpleSQLBackend(cb)
	if backend == nil {
		t.Fatal("expected non-nil backend")
	}

	_, _ = backend.HandleSimpleQuery(context.Background(), "SELECT 1")
	if !called {
		t.Fatal("expected callback to be called")
	}
}

func TestSimpleSQLBackend_HandleSimpleQuery(t *testing.T) {
	row := sqldata.NewSQLRow([]interface{}{"hello", 42})
	col := sqldata.NewSQLColumn(sqldata.NewSQLTable(0, ""), "name", 0, 25, 256, -1, "text")
	result := sqldata.NewSQLResult([]sqldata.ISQLColumn{col}, 0, 0, []sqldata.ISQLRow{row})
	stream := sqldata.NewSimpleSQLResultStream(result)

	var receivedQuery string
	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		receivedQuery = query
		return stream, nil
	}

	backend := NewSimpleSQLBackend(cb)
	got, err := backend.HandleSimpleQuery(context.Background(), "SELECT name FROM users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receivedQuery != "SELECT name FROM users" {
		t.Fatalf("got query %q, want %q", receivedQuery, "SELECT name FROM users")
	}

	res, err := got.Read()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if len(res.GetRows()) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.GetRows()))
	}
}

func TestSimpleSQLBackend_GetDebugStr(t *testing.T) {
	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		return nil, nil
	}
	backend := NewSimpleSQLBackend(cb)
	if got := backend.GetDebugStr(); got != "" {
		t.Fatalf("got debug string %q, want empty", got)
	}
}

func TestSimpleSQLBackend_SplitCompoundQuery(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "single statement",
			input: "SELECT 1",
			want:  []string{"SELECT 1"},
		},
		{
			name:  "two statements",
			input: "SELECT 1;SELECT 2",
			want:  []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:  "trailing semicolon",
			input: "SELECT 1;",
			want:  []string{"SELECT 1", ""},
		},
		{
			name:  "semicolon inside double quotes",
			input: `SELECT "col;name" FROM t`,
			want:  []string{`SELECT "col;name" FROM t`},
		},
		{
			name:  "empty input",
			input: "",
			want:  []string{""},
		},
		{
			name:  "multiple semicolons",
			input: "A;B;C",
			want:  []string{"A", "B", "C"},
		},
	}

	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		return nil, nil
	}
	backend := NewSimpleSQLBackend(cb)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := backend.SplitCompoundQuery(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d parts, want %d: %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("part[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestNewSimpleSQLBackendFactory(t *testing.T) {
	cb := func(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
		return nil, nil
	}

	factory := NewSimpleSQLBackendFactory(cb)
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}

	backend1, err := factory.NewSQLBackend()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	backend2, err := factory.NewSQLBackend()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// SimpleSQLBackendFactory returns the same instance each time
	if backend1 != backend2 {
		t.Fatal("expected factory to return same backend instance")
	}
}
