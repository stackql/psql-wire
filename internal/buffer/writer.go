package buffer

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/stackql/psql-wire/internal/types"
)

var (
	_ Writer = (*simpleWriter)(nil)
)

// Writer provides a convenient way to write pgwire protocol messages
type Writer interface {
	Start(types.ServerMessage)
	AddByte(byte)
	AddInt16(int16) int
	AddInt32(int32) int
	AddBytes([]byte) int
	AddString(string) int
	Bytes() []byte
	Error() error
	End() error
	Reset()
	AddNullTerminate()
	SetError(error)
}

type simpleWriter struct {
	io.Writer
	frame  bytes.Buffer
	putbuf [64]byte // buffer used to construct messages which could be written to the writer frame buffer
	err    error
}

// NewWriter constructs a new Postgres buffered message writer for the given io.Writer
func NewWriter(writer io.Writer) Writer {
	return &simpleWriter{
		Writer: writer,
	}
}

// Start resets the buffer writer and starts a new message with the given
// message type. The message type (byte) and reserved message length bytes (int32)
// are written to the underlaying bytes buffer.
func (writer *simpleWriter) Start(t types.ServerMessage) {
	writer.Reset()
	writer.putbuf[0] = byte(t)
	writer.frame.Write(writer.putbuf[:5]) // message type + message length
}

func (writer *simpleWriter) SetError(err error) {
	writer.err = err
}

// AddByte writes the given byte to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *simpleWriter) AddByte(b byte) {
	if writer.err != nil {
		return
	}

	writer.err = writer.frame.WriteByte(b)
}

// AddInt16 writes the given unsigned int16 to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *simpleWriter) AddInt16(i int16) (size int) {
	if writer.err != nil {
		return size
	}

	x := make([]byte, 2)
	binary.BigEndian.PutUint16(x, uint16(i))
	size, writer.err = writer.frame.Write(x)
	return size
}

// AddInt32 writes the given unsigned int32 to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *simpleWriter) AddInt32(i int32) (size int) {
	if writer.err != nil {
		return size
	}

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(i))
	size, writer.err = writer.frame.Write(x)
	return size
}

// AddBytes writes the given bytes to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *simpleWriter) AddBytes(b []byte) (size int) {
	if writer.err != nil {
		return size
	}

	size, writer.err = writer.frame.Write(b)
	return size
}

// AddString writes the given string to the writer frame. Bytes written to the
// frame could be read at any stage to interact with a Postgres client. Errors
// thrown while writing to the writer could be read by calling writer.Error()
func (writer *simpleWriter) AddString(s string) (size int) {
	if writer.err != nil {
		return size
	}

	size, writer.err = writer.frame.WriteString(s)
	return size
}

// AddNullTerminate writes a null terminate symbol to the end of the given data frame
func (writer *simpleWriter) AddNullTerminate() {
	if writer.err != nil {
		return
	}

	writer.err = writer.frame.WriteByte(0)
}

func (writer *simpleWriter) Error() error {
	return writer.err
}

// Bytes returns the written bytes to the active data frame
func (writer *simpleWriter) Bytes() []byte {
	return writer.frame.Bytes()
}

// Reset resets the data frame to be empty
func (writer *simpleWriter) Reset() {
	writer.frame.Reset()
	writer.err = nil
}

// End writes the prepared message to the given writer and resets the buffer.
// The to be expected message length is appended after the message status byte.
func (writer *simpleWriter) End() error {
	defer writer.Reset()
	if writer.Error() != nil {
		return writer.Error()
	}

	bytes := writer.frame.Bytes()
	length := uint32(writer.frame.Len() - 1) // total message length minus the message type byte
	binary.BigEndian.PutUint32(bytes[1:5], length)
	_, err := writer.Writer.Write(bytes)
	return err
}

// EncodeBoolean returns a string value ("on"/"off") representing the given boolean value
func EncodeBoolean(value bool) string {
	if value {
		return "on"
	}

	return "off"
}
