package state

import (
	"os"
	"strings"
	"time"

	"github.com/dbrest-io/dbrest/env"
	"github.com/flarco/dbio/database"
	"github.com/flarco/g"
	"gopkg.in/yaml.v3"
)

var (
	Roles           = RoleMap{}
	lastLoadedRoles time.Time
)

func init() {
	LoadRoles(true)
}

// Role is a map of Grants per connection
// each map key is a connection name
// each map item is a grant entry for that connection
type Role map[string]Grant

// RoleMap is a map of roles
// each map key is a role name
// each map item is a Role entry for that role
type RoleMap map[string]Role

func LoadRoles(force bool) (err error) {
	if !(force || time.Since(lastLoadedRoles) > (5*time.Second)) {
		return
	}

	if g.PathExists(env.HomeDirRolesFile) {
		var roles RoleMap
		rolesB, _ := os.ReadFile(env.HomeDirRolesFile)
		err = yaml.Unmarshal(rolesB, &roles)
		if err != nil {
			return g.Error(err, "could not load roles")
		}

		// make keys upper case
		for name, r := range roles {
			role := Role{}
			for k, grant := range r {
				role[strings.ToLower(k)] = grant
			}
			Roles[strings.ToLower(name)] = role
		}
		lastLoadedRoles = time.Now()
	}
	return
}

func GetRoleMap(roles []string) (rm RoleMap) {
	rm = RoleMap{}
	for _, rn := range roles {
		rn = strings.ToLower(rn)
		if role, ok := Roles[rn]; ok {
			rm[rn] = role
		}
	}
	return
}

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

func (rm RoleMap) CanSQL(connection string) bool {
	for _, role := range rm {
		if ok := role.CanSQL(connection); ok {
			return ok
		}
	}
	return false
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
