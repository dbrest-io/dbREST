package state

import (
	"github.com/flarco/dbio/database"
	"github.com/flarco/g"
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

// Role is a map of Grants per connection
// each map key is a connection name
// each map item is a grant entry for that connection
type Role map[string]Grant

// RoleMap is a map of roles
// each map key is a role name
// each map item is a Role entry for that role
type RoleMap map[string]Role

func (rm RoleMap) GetPermissions(connection string) (perms Permissions) {

	perms = Permissions{}
	for _, role := range rm {
		if grant, ok := role[connection]; ok {
			tables := grant.GetReadable(connection)
			for _, table := range tables {
				perms[table.FullName()] = PermissionRead
			}

			tables = grant.GetWritable(connection)
			for _, table := range tables {
				if p, ok := perms[table.FullName()]; ok {
					if p == PermissionRead {
						perms[table.FullName()] = PermissionReadWrite
					}
				} else {
					perms[table.FullName()] = PermissionWrite
				}
			}
		}
	}

	return
}

func (r Role) CanRead(connection string, table database.Table) bool {
	if grant, ok := r[connection]; ok {
		rTables := grant.GetReadable(connection)
		for _, rt := range rTables {
			if rt.Schema == "" && rt.Name == "*" {
				return true
			} else if rt.Schema == table.Schema && rt.Name == "*" {
				return true
			} else if rt.Schema == table.Schema && rt.Name == table.Name {
				return true
			}
		}
	}
	return false
}

func (r Role) CanWrite(connection string, table database.Table) bool {
	if grant, ok := r[connection]; ok {
		wTables := grant.GetWritable(connection)
		for _, wt := range wTables {
			if wt.Schema == "" && wt.Name == "*" {
				return true
			} else if wt.Schema == table.Schema && wt.Name == "*" {
				return true
			} else if wt.Schema == table.Schema && wt.Name == table.Name {
				return true
			}
		}
	}
	return false
}

func (r Role) CanSQL(connection string) bool {
	if grant, ok := r[connection]; ok {
		return grant.AllowSQL == AllowSQLAny
	}
	return false
}

func (gt Grant) GetReadable(connection string) (tables []database.Table) {
	// get connection type
	conn, err := GetConnObject(connection, "")
	if err != nil {
		return
	}

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

func (gt Grant) GetWritable(connection string) (tables []database.Table) {
	// get connection type
	conn, err := GetConnObject(connection, "")
	if err != nil {
		return
	}

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

// SchemaAll returns schema.*
// notation for all tables in a schema
func SchemaAll(connection, schema string) database.Table {
	conn, err := GetConnObject(connection, "")
	if err != nil {
		return database.Table{}
	}
	table, _ := database.ParseTableName(schema+".*", conn.Type)
	return table
}
