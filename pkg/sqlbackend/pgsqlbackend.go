package sqlbackend

import (
	"context"

	"github.com/stackql/psql-wire/pkg/sqldata"
)

type QueryCallback func(context.Context, string) (sqldata.ISQLResultStream, error)

type SQLBackendFactory interface {
	NewSQLBackend() (ISQLBackend, error)
}

type ISQLBackend interface {
	HandleSimpleQuery(context.Context, string) (sqldata.ISQLResultStream, error)
	SplitCompoundQuery(string) ([]string, error)
}

type SimpleSQLBackend struct {
	simpleCallback QueryCallback
}

func (sb *SimpleSQLBackend) CloneSQLBackend() ISQLBackend {
	return &SimpleSQLBackend{
		simpleCallback: sb.simpleCallback,
	}
}

func (sb *SimpleSQLBackend) HandleSimpleQuery(ctx context.Context, query string) (sqldata.ISQLResultStream, error) {
	return sb.simpleCallback(ctx, query)
}

func (sb *SimpleSQLBackend) SplitCompoundQuery(s string) ([]string, error) {
	res := []string{}
	var beg int
	var inDoubleQuotes bool

	for i := 0; i < len(s); i++ {
		if s[i] == ';' && !inDoubleQuotes {
			res = append(res, s[beg:i])
			beg = i + 1
		} else if s[i] == '"' {
			if !inDoubleQuotes {
				inDoubleQuotes = true
			} else if i > 0 && s[i-1] != '\\' {
				inDoubleQuotes = false
			}
		}
	}
	return append(res, s[beg:]), nil
}

func NewSimpleSQLBackend(simpleCallback QueryCallback) ISQLBackend {
	return &SimpleSQLBackend{
		simpleCallback: simpleCallback,
	}
}

type simpleSQLBackendFactory struct {
	sqlBackend ISQLBackend
}

func (sbf *simpleSQLBackendFactory) NewSQLBackend() (ISQLBackend, error) {
	return sbf.sqlBackend, nil
}

func NewSimpleSQLBackendFactory(simpleCallback QueryCallback) SQLBackendFactory {
	return &simpleSQLBackendFactory{
		sqlBackend: NewSimpleSQLBackend(simpleCallback),
	}
}
