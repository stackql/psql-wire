package sqlbackend

import (
	"context"

	"github.com/jeroenrinzema/psql-wire/pkg/sqldata"
)

type QueryCallback func(context.Context, string) (sqldata.ISQLResultStream, error)

type ISQLBackend interface {
	HandleSimpleQuery(context.Context, string) (sqldata.ISQLResultStream, error)
}

type SimpleSQLBackend struct {
	simpleCallback QueryCallback
}

func (sb *SimpleSQLBackend) HandleSimpleQuery(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
	return sb.simpleCallback(ctx, query)
}

func NewSimpleSQLBackend(simpleCallback QueryCallback) ISQLBackend {
	return &SimpleSQLBackend{
		simpleCallback: simpleCallback,
	}
}
