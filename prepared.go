package wire

// PreparedStatement represents a parsed SQL statement cached per-connection.
type PreparedStatement struct {
	Name      string
	Query     string
	ParamOIDs []uint32
}

// Portal represents a bound prepared statement with parameter values, ready for execution.
type Portal struct {
	Name          string
	Statement     *PreparedStatement
	ParamFormats  []int16
	ParamValues   [][]byte
	ResultFormats []int16
}

// PreparedStatementCache stores prepared statements for a connection.
type PreparedStatementCache map[string]*PreparedStatement

// PortalCache stores portals for a connection.
type PortalCache map[string]*Portal

func NewPreparedStatementCache() PreparedStatementCache {
	return make(PreparedStatementCache)
}

func NewPortalCache() PortalCache {
	return make(PortalCache)
}
