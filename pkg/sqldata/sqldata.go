package sqldata

import (
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
}

type SimpleSQLResultStream struct {
	res ISQLResult
}

func NewSimpleSQLResultStream(res ISQLResult) ISQLResultStream {
	return &SimpleSQLResultStream{
		res: res,
	}
}

func (srs *SimpleSQLResultStream) Read() (ISQLResult, error) {
	return srs.res, io.EOF
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
