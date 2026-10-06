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
	for _, supplied := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "supplied schema"}[supplied], func(t *testing.T) {
			stream := NewChannelSQLResultStream()
			var want []ISQLColumn
			if supplied {
				stream = NewChannelSQLResultStream(columns)
				want = columns
			}
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

			result := NewSQLResult(columns, 0, 0, nil)
			if err := stream.Write(result); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if !reflect.DeepEqual(stream.GetColumns(), want) {
					t.Fatal("GetColumns changed the constructor schema")
				}
			}
			got, err := stream.Read()
			if got != result || !errors.Is(err, io.EOF) {
				t.Fatalf("Read after GetColumns = (%v, %v), want original result and EOF", got, err)
			}
		})
	}
}
