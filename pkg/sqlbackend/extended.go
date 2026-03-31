package sqlbackend

import (
	"context"

	"github.com/stackql/psql-wire/pkg/sqldata"
)

// IExtendedQueryBackend provides extended query protocol support.
// Backends that implement this interface (in addition to ISQLBackend)
// can handle prepared statements, parameter binding, and describe operations.
//
// If an ISQLBackend does not implement this interface, the wire server
// will use a default implementation that delegates Execute to HandleSimpleQuery.
type IExtendedQueryBackend interface {
	// HandleParse prepares a SQL statement for later execution.
	// paramOIDs may contain zeros for unspecified parameter types.
	HandleParse(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, error)

	// HandleBind binds parameter values to a prepared statement, creating a portal.
	HandleBind(ctx context.Context, portalName string, stmtName string, paramFormats []int16, paramValues [][]byte, resultFormats []int16) error

	// HandleDescribeStatement returns parameter type OIDs and result column metadata
	// for a prepared statement.
	HandleDescribeStatement(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, []sqldata.ISQLColumn, error)

	// HandleDescribePortal returns result column metadata for a bound portal.
	HandleDescribePortal(ctx context.Context, portalName string, stmtName string, query string, paramOIDs []uint32) ([]sqldata.ISQLColumn, error)

	// HandleExecute executes a bound portal and returns a result stream.
	// maxRows of 0 means unlimited.
	HandleExecute(ctx context.Context, portalName string, stmtName string, query string, paramFormats []int16, paramValues [][]byte, resultFormats []int16, maxRows int32) (sqldata.ISQLResultStream, error)

	// HandleCloseStatement closes a prepared statement.
	HandleCloseStatement(ctx context.Context, stmtName string) error

	// HandleClosePortal closes a portal.
	HandleClosePortal(ctx context.Context, portalName string) error
}

// DefaultExtendedQueryBackend provides a default implementation of IExtendedQueryBackend
// that delegates execution to the simple query path. This is used when the backend
// does not natively support extended queries.
type DefaultExtendedQueryBackend struct {
	backend ISQLBackend
}

func NewDefaultExtendedQueryBackend(backend ISQLBackend) IExtendedQueryBackend {
	return &DefaultExtendedQueryBackend{backend: backend}
}

func (d *DefaultExtendedQueryBackend) HandleParse(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, error) {
	return paramOIDs, nil
}

func (d *DefaultExtendedQueryBackend) HandleBind(ctx context.Context, portalName string, stmtName string, paramFormats []int16, paramValues [][]byte, resultFormats []int16) error {
	return nil
}

func (d *DefaultExtendedQueryBackend) HandleDescribeStatement(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, []sqldata.ISQLColumn, error) {
	return paramOIDs, nil, nil
}

func (d *DefaultExtendedQueryBackend) HandleDescribePortal(ctx context.Context, portalName string, stmtName string, query string, paramOIDs []uint32) ([]sqldata.ISQLColumn, error) {
	return nil, nil
}

func (d *DefaultExtendedQueryBackend) HandleExecute(ctx context.Context, portalName string, stmtName string, query string, paramFormats []int16, paramValues [][]byte, resultFormats []int16, maxRows int32) (sqldata.ISQLResultStream, error) {
	return d.backend.HandleSimpleQuery(ctx, query)
}

func (d *DefaultExtendedQueryBackend) HandleCloseStatement(ctx context.Context, stmtName string) error {
	return nil
}

func (d *DefaultExtendedQueryBackend) HandleClosePortal(ctx context.Context, portalName string) error {
	return nil
}
