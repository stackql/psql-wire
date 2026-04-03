package wire

import (
	"context"
	"errors"
	"io"

	"github.com/lib/pq/oid"
	"github.com/stackql/psql-wire/internal/buffer"
	"github.com/stackql/psql-wire/internal/types"
	"github.com/stackql/psql-wire/pkg/sqldata"
	"go.uber.org/zap"
)

// handleParse handles the Parse message ('P') of the extended query protocol.
// It parses the SQL statement and caches it as a prepared statement.
func (srv *Server) handleParse(ctx context.Context, conn SQLConnection) error {
	stmtName, err := conn.GetString()
	if err != nil {
		return err
	}

	query, err := conn.GetString()
	if err != nil {
		return err
	}

	numParams, err := conn.GetUint16()
	if err != nil {
		return err
	}

	paramOIDs := make([]uint32, numParams)
	for i := 0; i < int(numParams); i++ {
		oidVal, err := conn.GetUint32()
		if err != nil {
			return err
		}
		paramOIDs[i] = oidVal
	}

	srv.logger.Debug("parse",
		zap.String("statement", stmtName),
		zap.String("query", query),
		zap.Int("params", int(numParams)),
	)

	extBackend := conn.ExtendedBackend()
	if extBackend != nil {
		resolvedOIDs, err := extBackend.HandleParse(ctx, stmtName, query, paramOIDs)
		if err != nil {
			return extendedError(conn, err)
		}
		paramOIDs = resolvedOIDs
	}

	conn.Statements()[stmtName] = &PreparedStatement{
		Name:      stmtName,
		Query:     query,
		ParamOIDs: paramOIDs,
	}

	return writeParseComplete(conn)
}

// handleBind handles the Bind message ('B') of the extended query protocol.
// It binds parameter values to a prepared statement, creating a portal.
func (srv *Server) handleBind(ctx context.Context, conn SQLConnection) error {
	portalName, err := conn.GetString()
	if err != nil {
		return err
	}

	stmtName, err := conn.GetString()
	if err != nil {
		return err
	}

	stmt, ok := conn.Statements()[stmtName]
	if !ok {
		return extendedError(conn, errors.New("prepared statement does not exist: "+stmtName))
	}

	// Read parameter format codes
	numParamFormats, err := conn.GetUint16()
	if err != nil {
		return err
	}
	paramFormats := make([]int16, numParamFormats)
	for i := 0; i < int(numParamFormats); i++ {
		v, err := conn.GetUint16()
		if err != nil {
			return err
		}
		paramFormats[i] = int16(v)
	}

	// Read parameter values
	numParams, err := conn.GetUint16()
	if err != nil {
		return err
	}
	paramValues := make([][]byte, numParams)
	for i := 0; i < int(numParams); i++ {
		length, err := conn.GetUint32()
		if err != nil {
			return err
		}
		// -1 indicates NULL
		if int32(length) == -1 {
			paramValues[i] = nil
		} else {
			val, err := conn.GetBytes(int(length))
			if err != nil {
				return err
			}
			paramValues[i] = val
		}
	}

	// Read result format codes
	numResultFormats, err := conn.GetUint16()
	if err != nil {
		return err
	}
	resultFormats := make([]int16, numResultFormats)
	for i := 0; i < int(numResultFormats); i++ {
		v, err := conn.GetUint16()
		if err != nil {
			return err
		}
		resultFormats[i] = int16(v)
	}

	srv.logger.Debug("bind",
		zap.String("portal", portalName),
		zap.String("statement", stmtName),
		zap.Int("params", int(numParams)),
	)

	extBackend := conn.ExtendedBackend()
	if extBackend != nil {
		err = extBackend.HandleBind(ctx, portalName, stmtName, paramFormats, paramValues, resultFormats)
		if err != nil {
			return extendedError(conn, err)
		}
	}

	conn.Portals()[portalName] = &Portal{
		Name:          portalName,
		Statement:     stmt,
		ParamFormats:  paramFormats,
		ParamValues:   paramValues,
		ResultFormats: resultFormats,
	}

	return writeBindComplete(conn)
}

// handleDescribe handles the Describe message ('D') of the extended query protocol.
// It returns metadata about a prepared statement or portal.
func (srv *Server) handleDescribe(ctx context.Context, conn SQLConnection) error {
	prepareType, err := conn.GetPrepareType()
	if err != nil {
		return err
	}

	name, err := conn.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("describe",
		zap.String("type", string(prepareType)),
		zap.String("name", name),
	)

	switch prepareType {
	case buffer.PrepareStatement:
		return srv.handleDescribeStatement(ctx, conn, name)
	case buffer.PreparePortal:
		return srv.handleDescribePortal(ctx, conn, name)
	default:
		return extendedError(conn, errors.New("unknown describe type"))
	}
}

func (srv *Server) handleDescribeStatement(ctx context.Context, conn SQLConnection, name string) error {
	stmt, ok := conn.Statements()[name]
	if !ok {
		return extendedError(conn, errors.New("prepared statement does not exist: "+name))
	}

	var paramOIDs []uint32
	var columns []sqldata.ISQLColumn

	extBackend := conn.ExtendedBackend()
	if extBackend != nil {
		var err error
		paramOIDs, columns, err = extBackend.HandleDescribeStatement(ctx, name, stmt.Query, stmt.ParamOIDs)
		if err != nil {
			return extendedError(conn, err)
		}
	}

	if paramOIDs == nil {
		paramOIDs = stmt.ParamOIDs
	}

	// Send ParameterDescription
	err := writeParameterDescription(conn, paramOIDs)
	if err != nil {
		return err
	}

	// Send RowDescription or NoData
	// Describe on a statement has no result formats yet (Bind hasn't happened)
	if columns != nil {
		return writeRowDescriptionFromSQLColumns(ctx, conn, columns, nil)
	}
	return writeNoData(conn)
}

func (srv *Server) handleDescribePortal(ctx context.Context, conn SQLConnection, name string) error {
	portal, ok := conn.Portals()[name]
	if !ok {
		return extendedError(conn, errors.New("portal does not exist: "+name))
	}

	var columns []sqldata.ISQLColumn

	extBackend := conn.ExtendedBackend()
	if extBackend != nil {
		var err error
		columns, err = extBackend.HandleDescribePortal(ctx, name, portal.Statement.Name, portal.Statement.Query, portal.Statement.ParamOIDs)
		if err != nil {
			return extendedError(conn, err)
		}
	}

	if columns != nil {
		return writeRowDescriptionFromSQLColumns(ctx, conn, columns, portal.ResultFormats)
	}
	return writeNoData(conn)
}

// handleExecute handles the Execute message ('E') of the extended query protocol.
// It executes a bound portal and returns result rows.
func (srv *Server) handleExecute(ctx context.Context, conn SQLConnection) error {
	portalName, err := conn.GetString()
	if err != nil {
		return err
	}

	maxRowsU32, err := conn.GetUint32()
	if err != nil {
		return err
	}
	maxRows := int32(maxRowsU32)

	srv.logger.Debug("execute",
		zap.String("portal", portalName),
		zap.Int32("maxRows", maxRows),
	)

	portal, ok := conn.Portals()[portalName]
	if !ok {
		return extendedError(conn, errors.New("portal does not exist: "+portalName))
	}

	extBackend := conn.ExtendedBackend()
	if extBackend == nil {
		return commandComplete(conn, "OK")
	}

	rdr, err := extBackend.HandleExecute(
		ctx,
		portalName,
		portal.Statement.Name,
		portal.Statement.Query,
		portal.ParamFormats,
		portal.ParamValues,
		portal.ResultFormats,
		maxRows,
	)
	if err != nil {
		return extendedError(conn, err)
	}

	if rdr == nil {
		return commandComplete(conn, "OK")
	}

	dw := &dataWriter{
		ctx:           ctx,
		client:        conn,
		resultFormats: portal.ResultFormats,
	}

	var headersWritten bool
	for {
		res, err := rdr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				notices := conn.GetDebugStr()
				if res == nil {
					dw.Complete(notices, "OK")
					return nil
				}
				if !headersWritten {
					headersWritten = true
					srv.writeSQLResultHeader(ctx, res, dw, portal.ResultFormats)
				}
				srv.writeSQLResultRows(ctx, res, dw)
				dw.Complete(notices, "OK")
				return nil
			}
			return extendedError(conn, err)
		}
		if !headersWritten {
			headersWritten = true
			// For extended query, we don't send RowDescription here if Describe already sent it.
			// However, the dataWriter.Define will handle this correctly since columns may already be set.
			dw.Define(nil)
		}
		srv.writeSQLResultRows(ctx, res, dw)
	}
}

// handleClose handles the Close message ('C') of the extended query protocol.
// It closes a prepared statement or portal.
func (srv *Server) handleClose(ctx context.Context, conn SQLConnection) error {
	prepareType, err := conn.GetPrepareType()
	if err != nil {
		return err
	}

	name, err := conn.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("close",
		zap.String("type", string(prepareType)),
		zap.String("name", name),
	)

	extBackend := conn.ExtendedBackend()

	switch prepareType {
	case buffer.PrepareStatement:
		if extBackend != nil {
			if err := extBackend.HandleCloseStatement(ctx, name); err != nil {
				return extendedError(conn, err)
			}
		}
		delete(conn.Statements(), name)
	case buffer.PreparePortal:
		if extBackend != nil {
			if err := extBackend.HandleClosePortal(ctx, name); err != nil {
				return extendedError(conn, err)
			}
		}
		delete(conn.Portals(), name)
	}

	return writeCloseComplete(conn)
}

// handleSync handles the Sync message ('S') of the extended query protocol.
// It signals the end of an extended query cycle and sends ReadyForQuery.
func (srv *Server) handleSync(ctx context.Context, conn SQLConnection) error {
	return readyForQuery(conn, types.ServerIdle)
}

// handleFlush handles the Flush message ('H') of the extended query protocol.
// It ensures all pending output has been sent to the client.
// Since our writer sends immediately on End(), this is effectively a no-op.
func (srv *Server) handleFlush(ctx context.Context, conn SQLConnection) error {
	return nil
}

// extendedError sends an ErrorResponse to the client and returns errExtendedQueryError
// so the command loop enters error state (discards messages until Sync).
func extendedError(writer buffer.Writer, err error) error {
	ErrorCode(writer, err)
	return errExtendedQueryError
}

// Wire protocol response helpers

func writeParseComplete(writer buffer.Writer) error {
	writer.Start(types.ServerParseComplete)
	return writer.End()
}

func writeBindComplete(writer buffer.Writer) error {
	writer.Start(types.ServerBindComplete)
	return writer.End()
}

func writeCloseComplete(writer buffer.Writer) error {
	writer.Start(types.ServerCloseComplete)
	return writer.End()
}

func writeNoData(writer buffer.Writer) error {
	writer.Start(types.ServerNoData)
	return writer.End()
}

func writeParameterDescription(writer buffer.Writer, paramOIDs []uint32) error {
	writer.Start(types.ServerParameterDescription)
	writer.AddInt16(int16(len(paramOIDs)))
	for _, paramOID := range paramOIDs {
		writer.AddInt32(int32(paramOID))
	}
	return writer.End()
}

func writeRowDescriptionFromSQLColumns(ctx context.Context, writer buffer.Writer, columns []sqldata.ISQLColumn, resultFormats []int16) error {
	var colz Columns
	for i, c := range columns {
		colz = append(colz, Column{
			Table:  c.GetTableId(),
			Name:   c.GetName(),
			AttrNo: c.GetAttrNum(),
			Oid:    oid.Oid(c.GetObjectID()),
			Width:  c.GetWidth(),
			Format: resolveResultFormat(resultFormats, i),
		})
	}
	return colz.Define(ctx, writer)
}

// resolveResultFormat determines the format code for column i based on the
// result format codes from the Bind message, per the PostgreSQL protocol:
//   - 0 format codes: all columns use text
//   - 1 format code:  all columns use that format
//   - N format codes: each column uses its corresponding format
func resolveResultFormat(formats []int16, i int) FormatCode {
	if len(formats) == 0 {
		return TextFormat
	}
	if len(formats) == 1 {
		return FormatCode(formats[0])
	}
	if i < len(formats) {
		return FormatCode(formats[i])
	}
	return TextFormat
}
