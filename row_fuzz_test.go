package wire

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/lib/pq/oid"
	"github.com/stackql/psql-wire/internal/buffer"
)

func FuzzRowEncoding(f *testing.F) {
	for _, registeredOID := range fuzzRegisteredOIDs() {
		f.Add(registeredOID, uint8(TextFormat), []byte{1})
		f.Add(registeredOID, uint8(BinaryFormat), []byte{1})
	}
	f.Add(uint32(oid.T_text), uint8(TextFormat), []byte("fuzz"))
	f.Add(uint32(oid.T_int4), uint8(BinaryFormat), []byte{0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, oidValue uint32, format uint8, input []byte) {
		ctx := setTypeInfo(context.Background())
		column := Column{Oid: oid.Oid(oidValue), Format: FormatCode(format % 2)}
		var output bytes.Buffer
		writer := buffer.NewWriter(&output)
		var value interface{}
		if len(input) > 0 {
			switch input[0] % 5 {
			case 0:
				value = nil
			case 1:
				value = string(input)
			case 2:
				value = input
			case 3:
				value = int64(len(input))
			case 4:
				value = struct{}{}
			}
		}
		_ = column.Write(ctx, writer, value)
		_ = (Columns{column}).Write(ctx, writer, []interface{}{value})

		dataType := &pgtype.DataType{Name: "unsupported", OID: oidValue, Value: &fuzzUnsupportedType{}}
		encoder := column.Format.Encoder(dataType)
		_, _ = encoder(TypeInfo(ctx), nil)

		encoder = column.Format.Encoder(nil)
		_, _ = encoder(TypeInfo(ctx), nil)
	})
}

type fuzzUnsupportedType struct{}

func (*fuzzUnsupportedType) Set(interface{}) error      { return nil }
func (*fuzzUnsupportedType) Get() interface{}           { return nil }
func (*fuzzUnsupportedType) AssignTo(interface{}) error { return nil }

func fuzzRegisteredOIDs() []uint32 {
	connInfo := pgtype.NewConnInfo()
	oids := reflect.ValueOf(connInfo).Elem().FieldByName("oidToDataType")
	if !oids.IsValid() || oids.Kind() != reflect.Map {
		panic(fmt.Sprintf("pgtype ConnInfo OID registry unavailable: %T", connInfo))
	}
	keys := oids.MapKeys()
	values := make([]uint32, 0, len(keys))
	for _, key := range keys {
		values = append(values, uint32(key.Uint()))
	}
	return values
}
