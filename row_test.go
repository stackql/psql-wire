package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/lib/pq/oid"
	"github.com/stackql/psql-wire/internal/buffer"
)

// readDataRowValues reads DataRow column values from raw bytes written by Columns.Write.
// Returns the raw byte slices for each column (nil for NULL).
func readDataRowValues(t *testing.T, data []byte) [][]byte {
	t.Helper()
	r := bytes.NewReader(data)

	// ServerDataRow type byte
	msgType := make([]byte, 1)
	if _, err := r.Read(msgType); err != nil {
		t.Fatalf("read type byte: %v", err)
	}

	// message length (int32)
	var msgLen int32
	if err := binary.Read(r, binary.BigEndian, &msgLen); err != nil {
		t.Fatalf("read msg length: %v", err)
	}

	// column count (int16)
	var numCols int16
	if err := binary.Read(r, binary.BigEndian, &numCols); err != nil {
		t.Fatalf("read column count: %v", err)
	}

	values := make([][]byte, numCols)
	for i := 0; i < int(numCols); i++ {
		var length int32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			t.Fatalf("read value length for col %d: %v", i, err)
		}
		if length == -1 {
			values[i] = nil
		} else {
			val := make([]byte, length)
			if _, err := r.Read(val); err != nil {
				t.Fatalf("read value for col %d: %v", i, err)
			}
			values[i] = val
		}
	}
	return values
}

func TestAsTextBytes(t *testing.T) {
	tests := []struct {
		name     string
		src      interface{}
		wantData []byte
		wantOK   bool
	}{
		{
			name:     "string value",
			src:      "hello",
			wantData: []byte("hello"),
			wantOK:   true,
		},
		{
			name:     "byte slice",
			src:      []byte("world"),
			wantData: []byte("world"),
			wantOK:   true,
		},
		{
			name:     "null string",
			src:      "null",
			wantData: nil,
			wantOK:   true,
		},
		{
			name:     "NULL string uppercase",
			src:      "NULL",
			wantData: nil,
			wantOK:   true,
		},
		{
			name:     "integer not matched",
			src:      42,
			wantData: nil,
			wantOK:   false,
		},
		{
			name:     "bool not matched",
			src:      true,
			wantData: nil,
			wantOK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, ok := asTextBytes(tt.src)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !bytes.Equal(data, tt.wantData) {
				t.Fatalf("data = %q, want %q", data, tt.wantData)
			}
		})
	}
}

func TestColumnWrite_TextBypass_PreservesExactString(t *testing.T) {
	ctx := setTypeInfo(context.Background())

	tests := []struct {
		name     string
		colOid   oid.Oid
		src      interface{}
		wantText string
	}{
		{
			name:     "bool column with sqlite t/f preserved",
			colOid:   oid.T_bool,
			src:      "t",
			wantText: "t",
		},
		{
			name:     "bool column with 1/0 preserved",
			colOid:   oid.T_bool,
			src:      "1",
			wantText: "1",
		},
		{
			name:     "int8 column with string preserved exactly",
			colOid:   oid.T_int8,
			src:      "100000001",
			wantText: "100000001",
		},
		{
			name:     "text column with plain string",
			colOid:   oid.T_text,
			src:      "hello world",
			wantText: "hello world",
		},
		{
			name:     "int4 column with byte slice",
			colOid:   oid.T_int4,
			src:      []byte("42"),
			wantText: "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := Column{
				Name:   "test",
				Oid:    tt.colOid,
				Width:  -1,
				Format: TextFormat,
			}

			var buf bytes.Buffer
			writer := buffer.NewWriter(&buf)

			// Wrap in a DataRow message to get valid framing
			cols := Columns{col}
			err := cols.Write(ctx, writer, []interface{}{tt.src})
			if err != nil {
				t.Fatalf("Write error: %v", err)
			}

			values := readDataRowValues(t, buf.Bytes())
			if len(values) != 1 {
				t.Fatalf("got %d values, want 1", len(values))
			}

			got := string(values[0])
			if got != tt.wantText {
				t.Errorf("got %q, want %q (OID %d)", got, tt.wantText, tt.colOid)
			}
		})
	}
}

func TestColumnWrite_TextBypass_NullHandling(t *testing.T) {
	ctx := setTypeInfo(context.Background())

	col := Column{
		Name:   "test",
		Oid:    oid.T_text,
		Width:  -1,
		Format: TextFormat,
	}

	var buf bytes.Buffer
	writer := buffer.NewWriter(&buf)

	cols := Columns{col}
	err := cols.Write(ctx, writer, []interface{}{"null"})
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	values := readDataRowValues(t, buf.Bytes())
	if values[0] != nil {
		t.Errorf("expected NULL (nil), got %q", values[0])
	}
}

func TestColumnWrite_NonString_UsesStandardEncoder(t *testing.T) {
	ctx := setTypeInfo(context.Background())

	// When source is a native Go type (not string/[]byte),
	// the standard pgtype encoder should be used.
	col := Column{
		Name:   "test",
		Oid:    oid.T_int4,
		Width:  4,
		Format: TextFormat,
	}

	var buf bytes.Buffer
	writer := buffer.NewWriter(&buf)

	cols := Columns{col}
	err := cols.Write(ctx, writer, []interface{}{int32(42)})
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}

	values := readDataRowValues(t, buf.Bytes())
	got := string(values[0])
	if got != "42" {
		t.Errorf("got %q, want %q", got, "42")
	}
}

func TestResolveResultFormat(t *testing.T) {
	tests := []struct {
		name    string
		formats []int16
		index   int
		want    FormatCode
	}{
		{
			name:    "empty formats defaults to text",
			formats: nil,
			index:   0,
			want:    TextFormat,
		},
		{
			name:    "single format applies to all columns",
			formats: []int16{1},
			index:   0,
			want:    BinaryFormat,
		},
		{
			name:    "single format applies to column 5",
			formats: []int16{1},
			index:   5,
			want:    BinaryFormat,
		},
		{
			name:    "per-column format code 0",
			formats: []int16{0, 1, 0},
			index:   0,
			want:    TextFormat,
		},
		{
			name:    "per-column format code 1",
			formats: []int16{0, 1, 0},
			index:   1,
			want:    BinaryFormat,
		},
		{
			name:    "index beyond formats defaults to text",
			formats: []int16{1, 0},
			index:   5,
			want:    TextFormat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveResultFormat(tt.formats, tt.index)
			if got != tt.want {
				t.Errorf("resolveResultFormat(%v, %d) = %d, want %d", tt.formats, tt.index, got, tt.want)
			}
		})
	}
}
