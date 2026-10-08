package wire

import (
	"fmt"

	"github.com/jackc/pgtype"
)

// FormatCode represents the encoding format of a given column
type FormatCode int16

// Encoder returns the format encoder for the given data type
func (code FormatCode) Encoder(t *pgtype.DataType) FormatEncoder {
	if t == nil || t.Value == nil {
		return unknownEncoderfunc(fmt.Errorf("format %d has no data type value", code))
	}

	switch code {
	case TextFormat:
		encoder, ok := t.Value.(pgtype.TextEncoder)
		if !ok {
			return unknownEncoderfunc(fmt.Errorf("data type %q does not support text encoding", t.Name))
		}
		return encoder.EncodeText
	case BinaryFormat:
		encoder, ok := t.Value.(pgtype.BinaryEncoder)
		if !ok {
			return unknownEncoderfunc(fmt.Errorf("data type %q does not support binary encoding", t.Name))
		}
		return encoder.EncodeBinary
	default:
		return unknownEncoderfunc(fmt.Errorf("unknown format encoder %d", code))
	}
}

// FormatEncoder represents a format code wire encoder.
// FormatEncoder should append the text format of self to buf. If self is the
// SQL value NULL then append nothing and return (nil, nil). The caller of
// FormatEncoder is responsible for writing the correct NULL value or the
// length of the data written.
type FormatEncoder func(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error)

func unknownEncoderfunc(err error) FormatEncoder {
	return func(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
		return nil, err
	}
}

const (
	// TextFormat is the default, text format.
	TextFormat FormatCode = 0
	// BinaryFormat is an alternative, binary, encoding.
	BinaryFormat FormatCode = 1
)
