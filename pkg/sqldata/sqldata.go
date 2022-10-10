package sqldata

import (
	"fmt"
	"io"
)

type ISQLResult interface {
	GetColumns() []ISQLColumn
	GetRowsAffected() uint64
	GetInsertId() uint64
	GetRows() []ISQLRow
}

type ISQLResultStream interface {
	Read() (ISQLResult, error)
	Write(ISQLResult) error
	Close() error
}

type SimpleSQLResultStream struct {
	res ISQLResult
}

type ChannelSQLResultStream struct {
	res              chan ISQLResult
	nextResultCached ISQLResult
}

func NewSimpleSQLResultStream(res ISQLResult) ISQLResultStream {
	return &SimpleSQLResultStream{
		res: res,
	}
}

func NewChannelSQLResultStream() ISQLResultStream {
	return &ChannelSQLResultStream{
		res: make(chan ISQLResult, 1),
	}
}

func (srs *SimpleSQLResultStream) Read() (ISQLResult, error) {
	return srs.res, io.EOF
}

func (srs *SimpleSQLResultStream) Write(r ISQLResult) error {
	return fmt.Errorf("not implemented")
}

func (srs *ChannelSQLResultStream) Read() (ISQLResult, error) {
	var rv ISQLResult
	var err error
	var ok bool
	if srs.nextResultCached != nil {
		rv = srs.nextResultCached
		srs.nextResultCached, ok = <-srs.res
	} else {
		rv, ok = <-srs.res
		if ok {
			srs.nextResultCached, ok = <-srs.res
		}
	}
	if !ok {
		err = io.EOF
	}
	return rv, err
}

func (srs *SimpleSQLResultStream) Close() error {
	return fmt.Errorf("not implemented")
}

func (srs *ChannelSQLResultStream) Close() error {
	close(srs.res)
	return nil
}

func (srs *ChannelSQLResultStream) Write(r ISQLResult) error {
	srs.res <- r
	return nil
}

type SQLResult struct {
	columns      []ISQLColumn
	rowsAffected uint64
	insertID     uint64
	rows         []ISQLRow
}

func NewSQLResult(
	columns []ISQLColumn,
	rowsAffected uint64,
	insertID uint64,
	rows []ISQLRow,
) *SQLResult {
	return &SQLResult{
		columns:      columns,
		rowsAffected: rowsAffected,
		insertID:     insertID,
		rows:         rows,
	}
}

func (sr *SQLResult) GetColumns() []ISQLColumn {
	return sr.columns
}

func (sr *SQLResult) GetRowsAffected() uint64 {
	return sr.rowsAffected
}

func (sr *SQLResult) GetInsertId() uint64 {
	return sr.insertID
}

func (sr *SQLResult) GetRows() []ISQLRow {
	return sr.rows
}

type ISQLTable interface {
	GetId() int32
	GetName() string
}

type SQLTable struct {
	id   int32
	name string
}

func NewSQLTable(id int32, name string) ISQLTable {
	return &SQLTable{id: id, name: name}
}

func (st *SQLTable) GetId() int32 {
	return st.id
}

func (st *SQLTable) GetName() string {
	return st.name
}

type ISQLColumn interface {
	GetTableId() int32
	GetName() string
	GetAttrNum() int16
	GetObjectID() uint32
	GetWidth() int16
	GetTypeModifier() int32
	GetFormat() string
}

type SQLColumn struct {
	table        ISQLTable
	name         string
	attrNum      int16
	objectID     uint32
	width        int16
	typeModifier int32
	format       string
}

func NewSQLColumn(
	table ISQLTable,
	name string,
	attrNum int16,
	objectID uint32,
	width int16,
	typeModifier int32,
	format string,
) ISQLColumn {
	return &SQLColumn{
		table:        table,
		name:         name,
		attrNum:      attrNum,
		objectID:     objectID,
		width:        width,
		typeModifier: typeModifier,
		format:       format,
	}
}

func (sc *SQLColumn) GetTableId() int32 {
	return sc.table.GetId()
}

func (sc *SQLColumn) GetTypeModifier() int32 {
	return sc.typeModifier
}

func (sc *SQLColumn) GetObjectID() uint32 {
	return sc.objectID
}

func (sc *SQLColumn) GetAttrNum() int16 {
	return sc.attrNum
}

func (sc *SQLColumn) GetWidth() int16 {
	return sc.width
}

func (sc *SQLColumn) GetName() string {
	return sc.name
}

func (sc *SQLColumn) GetFormat() string {
	return sc.format
}

type ISQLRow interface {
	GetRowDataNaive() []interface{}
	GetRowDataForPgWire() []interface{}
}

type SQLRow struct {
	rawData []interface{}
}

func NewSQLRow(rawData []interface{}) ISQLRow {
	return &SQLRow{
		rawData: rawData,
	}
}

func (sr *SQLRow) GetRowDataNaive() []interface{} {
	return sr.rawData
}

func (sr *SQLRow) GetRowDataForPgWire() []interface{} {
	var rv []interface{}
	for _, val := range sr.rawData {
		switch v := val.(type) {
		case []uint8:
			rv = append(rv, string(v))
		default:
			rv = append(rv, v)
		}
	}
	return rv
}
