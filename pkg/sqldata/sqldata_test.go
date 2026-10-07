package sqldata

import (
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestSimpleSQLResultStreamGetColumns(t *testing.T) {
	columns := []ISQLColumn{
		NewSQLColumn(NewSQLTable(0, ""), "id", 0, 23, 4, -1, "text"),
	}
	result := NewSQLResult(columns, 0, 0, nil)
	stream := NewSimpleSQLResultStream(result)
	for i := 0; i < 2; i++ {
		if !reflect.DeepEqual(stream.GetColumns(), columns) {
			t.Fatal("GetColumns did not return the result schema")
		}
	}
	got, err := stream.Read()
	if got != result || !errors.Is(err, io.EOF) {
		t.Fatalf("Read after GetColumns = (%v, %v), want original result and EOF", got, err)
	}
	if NewSimpleSQLResultStream(nil).GetColumns() != nil {
		t.Fatal("nil result should have nil schema")
	}
}

func TestChannelSQLResultStreamGetColumns(t *testing.T) {
	columns := []ISQLColumn{
		NewSQLColumn(NewSQLTable(0, ""), "id", 0, 23, 4, -1, "text"),
	}
	for _, want := range [][]ISQLColumn{columns, {}} {
		t.Run(map[bool]string{false: "zero columns", true: "columns"}[len(want) > 0], func(t *testing.T) {
			stream := NewChannelSQLResultStream(NewSQLResult(want, 0, 0, nil))

			done := make(chan []ISQLColumn, 1)
			go func() { done <- stream.GetColumns() }()
			select {
			case got := <-done:
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("GetColumns = %v, want %v", got, want)
				}
			case <-time.After(time.Second):
				t.Fatal("GetColumns blocked on an empty channel")
			}

			result := NewSQLResult(want, 0, 0, nil)
			if err := stream.Write(result); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if !reflect.DeepEqual(stream.GetColumns(), want) {
					t.Fatal("GetColumns changed the provider schema")
				}
			}
			got, err := stream.Read()
			if got != result || !errors.Is(err, io.EOF) {
				t.Fatalf("Read after GetColumns = (%v, %v), want original result and EOF", got, err)
			}
		})
	}
}

type countingColumnProvider struct {
	columns []ISQLColumn
	calls   int
}

func (provider *countingColumnProvider) GetColumns() []ISQLColumn {
	provider.calls++
	return provider.columns
}

func TestChannelSQLResultStreamDelegatesGetColumns(t *testing.T) {
	provider := &countingColumnProvider{}
	stream := NewChannelSQLResultStream(provider)
	if provider.calls != 0 {
		t.Fatal("constructor called the column provider")
	}
	if stream.GetColumns() != nil || provider.calls != 1 {
		t.Fatal("GetColumns did not delegate to the provider")
	}
	provider.columns = []ISQLColumn{
		NewSQLColumn(NewSQLTable(0, ""), "id", 0, 23, 4, -1, "text"),
	}
	result := NewSQLResult(provider.columns, 0, 0, nil)
	if err := stream.Write(result); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if !reflect.DeepEqual(stream.GetColumns(), provider.columns) {
			t.Fatal("GetColumns returned a stale schema")
		}
	}
	if provider.calls != 3 {
		t.Fatalf("provider called %d times, want 3", provider.calls)
	}
	got, err := stream.Read()
	if got != result || !errors.Is(err, io.EOF) {
		t.Fatalf("Read after GetColumns = (%v, %v), want original result and EOF", got, err)
	}
	if NewChannelSQLResultStream(nil).GetColumns() != nil {
		t.Fatal("nil provider should have nil schema")
	}
}
