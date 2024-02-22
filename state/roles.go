package state

import (
	"strings"

	"github.com/slingdata-io/sling-cli/core/dbio/connection"
)

var (
	AllowAllRoleMap = RoleMap{
		"*": Role{
			"*": Grant{
				AllowRead:  []string{"*"},
				AllowWrite: []string{"*"},
				AllowSQL:   AllowSQLAny,
			},
		},
	}
)

// Role is a map of Grants per connection
// each map key is a connection name
// each map item is a grant entry for that connection
type Role map[string]Grant

// RoleMap is a map of roles
// each map key is a role name
// each map item is a Role entry for that role
type RoleMap map[string]Role

func (rm RoleMap) HasAccess(connection string) bool {
	for _, role := range rm {
		if _, ok := role[connection]; ok {
			return true
		} else if _, ok := role["*"]; ok {
			return true
		}
	}
	return false
}

func (rm RoleMap) GetPermissions(conn connection.Connection) (perms Permissions) {
	perms = Permissions{}
	for _, role := range rm {
		grant, ok := role[strings.ToLower(conn.Name)]
		if !ok {
			grant, ok = role["*"]
		}

		if ok {
			tables := grant.GetReadable(conn)
			for _, table := range tables {
				perms[table.FullName()] = PermissionRead
			}

			tables = grant.GetWritable(conn)
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

func (rm RoleMap) CanSQL(connection string) bool {
	for _, role := range rm {
		if ok := role.CanSQL(connection); ok {
			return ok
		}
	}
	return false
}

func (r Role) CanSQL(connection string) bool {
	if grant, ok := r[connection]; ok {
		return grant.AllowSQL == AllowSQLAny
	} else if grant, ok := r["*"]; ok {
		return grant.AllowSQL == AllowSQLAny
	}
	return false
}
