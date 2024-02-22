package state

import (
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
)

type Grant struct {
	// AllowRead lists the schema/tables that are allowed to be read from
	AllowRead []string `json:"allow_read" yaml:"allow_read"`
	// AllowWrite lists the schema/tables that are allowed to be written to
	AllowWrite []string `json:"allow_write" yaml:"allow_write"`
	// AllowSQL shows whether a
	AllowSQL AllowSQLValue `json:"allow_sql" yaml:"allow_sql"`
}

// Permissions is a map of all objects for one connection
type Permissions map[string]Permission

type Permission string

const (
	PermissionNone      Permission = "none"
	PermissionRead      Permission = "read"
	PermissionWrite     Permission = "write"
	PermissionReadWrite Permission = "read_write"
)

func (p Permission) CanRead() bool {
	return p == PermissionRead || p == PermissionReadWrite
}

func (p Permission) CanWrite() bool {
	return p == PermissionWrite || p == PermissionReadWrite
}

type AllowSQLValue string

const (
	AllowSQLDisable AllowSQLValue = "disable"
	AllowSQLAny     AllowSQLValue = "any"
	// AllowSQLOnlySelect AllowSQLValue = "only_select"
)

func (gt Grant) GetReadable(conn connection.Connection) (tables []database.Table) {
	for _, t := range gt.AllowRead {
		table, err := database.ParseTableName(t, conn.Type)
		if err != nil {
			g.Warn("could not parse table entry: %s", t)
			continue
		}
		tables = append(tables, table)
	}
	return
}

func (gt Grant) GetWritable(conn connection.Connection) (tables []database.Table) {
	for _, t := range gt.AllowWrite {
		table, err := database.ParseTableName(t, conn.Type)
		if err != nil {
			g.Warn("could not parse table entry: %s", t)
			continue
		}
		tables = append(tables, table)
	}
	return
}
