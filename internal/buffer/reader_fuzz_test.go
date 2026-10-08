package buffer

import (
	"bytes"
	"testing"
)

func FuzzReader(f *testing.F) {
	f.Add([]byte{0xff})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{0, 0, 0, 4, 'Q', 0, 0, 0})

	f.Fuzz(func(t *testing.T, input []byte) {
		reader := CreateTestReader(input, nil)
		if len(input) > 0 {
			_, _ = reader.GetBytes(int(int8(input[0])))
		}
		for i, op := range input {
			switch op % 4 {
			case 0:
				n := int(int8(op))
				_, _ = reader.GetBytes(n)
			case 1:
				_, _ = reader.GetString()
			case 2:
				_, _ = reader.GetUint16()
			case 3:
				_, _ = reader.GetUint32()
			}
			if i >= 127 {
				break
			}
		}

		untyped := NewReader(bytes.NewReader(input), 256)
		_, _ = untyped.ReadUntypedMsg()
		typed := NewReader(bytes.NewReader(input), 256)
		_, _, _ = typed.ReadTypedMsg()
	})
}
