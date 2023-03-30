package wire

import (
	"net"
	"time"

	"github.com/stackql/psql-wire/internal/buffer"
	"github.com/stackql/psql-wire/internal/types"
	"github.com/stackql/psql-wire/pkg/sqlbackend"
)

var (
	_ SQLConnection = &simpleSqlConnection{}
)

type SQLConnection interface {
	net.Conn
	buffer.Reader
	buffer.Writer
	ID() uint64
}

type simpleSqlConnection struct {
	id         uint64
	netConn    net.Conn
	reader     buffer.Reader
	writer     buffer.Writer
	sqlBackend sqlbackend.ISQLBackend
}

func NewSQLConnection(
	id uint64,
	netConn net.Conn,
	reader buffer.Reader,
	writer buffer.Writer,
	sqlBackend sqlbackend.ISQLBackend,
) SQLConnection {
	return &simpleSqlConnection{
		id:         id,
		netConn:    netConn,
		reader:     reader,
		writer:     writer,
		sqlBackend: sqlBackend,
	}
}

func (c *simpleSqlConnection) ResetReader(size int) {
	c.reader.ResetReader(size)
}

func (c *simpleSqlConnection) PeekMsg() []byte {
	return c.reader.PeekMsg()
}

func (c *simpleSqlConnection) GetPrepareType() (buffer.PrepareType, error) {
	return c.reader.GetPrepareType()
}

func (c *simpleSqlConnection) SetError(err error) {
	c.writer.SetError(err)
}

func (c *simpleSqlConnection) Bytes() []byte {
	return c.writer.Bytes()
}

func (c *simpleSqlConnection) Error() error {
	return c.writer.Error()
}

func (c *simpleSqlConnection) Read(b []byte) (int, error) {
	return c.netConn.Read(b)
}

func (c *simpleSqlConnection) Write(b []byte) (int, error) {
	return c.netConn.Write(b)
}

func (c *simpleSqlConnection) Close() error {
	return c.netConn.Close()
}

func (c *simpleSqlConnection) LocalAddr() net.Addr {
	return c.netConn.LocalAddr()
}

func (c *simpleSqlConnection) RemoteAddr() net.Addr {
	return c.netConn.RemoteAddr()
}

func (c *simpleSqlConnection) SetDeadline(t time.Time) error {
	return c.netConn.SetDeadline(t)
}

func (c *simpleSqlConnection) SetReadDeadline(t time.Time) error {
	return c.netConn.SetReadDeadline(t)
}

func (c *simpleSqlConnection) SetWriteDeadline(t time.Time) error {
	return c.netConn.SetWriteDeadline(t)
}

func (c *simpleSqlConnection) ID() uint64 {
	return c.id
}

func (c *simpleSqlConnection) GetString() (string, error) {
	return c.reader.GetString()
}

func (c *simpleSqlConnection) GetBytes(size int) ([]byte, error) {
	return c.reader.GetBytes(size)
}

func (c *simpleSqlConnection) GetUint16() (uint16, error) {
	return c.reader.GetUint16()
}

func (c *simpleSqlConnection) GetUint32() (uint32, error) {
	return c.reader.GetUint32()
}

func (c *simpleSqlConnection) ReadTypedMsg() (types.ClientMessage, int, error) {
	return c.reader.ReadTypedMsg()
}

func (c *simpleSqlConnection) ReadUntypedMsg() (int, error) {
	return c.reader.ReadUntypedMsg()
}

func (c *simpleSqlConnection) Slurp(size int) error {
	return c.reader.Slurp(size)
}

func (c *simpleSqlConnection) Start(m types.ServerMessage) {
	c.writer.Start(m)
}

func (c *simpleSqlConnection) AddByte(b byte) {
	c.writer.AddByte(b)
}

func (c *simpleSqlConnection) AddBytes(b []byte) int {
	return c.writer.AddBytes(b)
}

func (c *simpleSqlConnection) AddString(s string) int {
	return c.writer.AddString(s)
}

func (c *simpleSqlConnection) AddInt16(i int16) int {
	return c.writer.AddInt16(i)
}

func (c *simpleSqlConnection) AddInt32(i int32) int {
	return c.writer.AddInt32(i)
}

func (c *simpleSqlConnection) Reset() {
	c.writer.Reset()
}

func (c *simpleSqlConnection) AddNullTerminate() {
	c.writer.AddNullTerminate()
}

func (c *simpleSqlConnection) End() error {
	return c.writer.End()
}
