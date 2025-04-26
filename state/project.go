package state

import (
	"context"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/dbrest-io/dbrest/env"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

var DefaultProjectID = "default"

var Projects = map[string]*Project{}

type Project struct {
	ID        string
	Directory string

	Connections map[string]*Connection
	Queries     map[string]*Query
	Tokens      TokenMap
	TokenValues map[string]Token

	Roles         RoleMap
	NoRestriction bool

	EnvFile   string
	TokenFile string
	RolesFile string

	mux sync.Mutex

	lastLoadedConns  time.Time
	lastLoadedRoles  time.Time
	lastLoadedTokens time.Time
}

func DefaultProject() (proj *Project) {
	if DefaultProjectID == "" {
		return nil
	}

	if proj = LoadProject(DefaultProjectID); proj != nil {
		return proj
	}

	noRestriction := false
	if val := os.Getenv("DBREST_NO_RESTRICTION"); val != "" {
		noRestriction = cast.ToBool(val)
	}

	return NewProject(DefaultProjectID, env.HomeDir, noRestriction)
}

func NewProject(id, directory string, noRestriction bool) (proj *Project) {
	mux.Lock()
	defer mux.Unlock()

	p := &Project{
		ID:               id,
		Directory:        directory,
		Connections:      map[string]*Connection{},
		Queries:          map[string]*Query{},
		Tokens:           TokenMap{},
		TokenValues:      map[string]Token{},
		Roles:            RoleMap{},
		NoRestriction:    noRestriction,
		EnvFile:          path.Join(directory, "env.yaml"),
		TokenFile:        path.Join(directory, ".tokens"),
		RolesFile:        path.Join(directory, "roles.yaml"),
		mux:              sync.Mutex{},
		lastLoadedRoles:  time.Unix(0, 0),
		lastLoadedTokens: time.Unix(0, 0),
	}

	p.LoadTokens(true)
	p.LoadRoles(true)
	p.LoadConnections(true)

	Projects[id] = p

	return p
}

func LoadProject(id string) (proj *Project) {
	mux.Lock()
	defer mux.Unlock()

	if p, ok := Projects[id]; ok {
		p.LoadTokens(false)
		p.LoadRoles(false)
		p.LoadConnections(false)

		return p
	}

	return nil
}

func (p *Project) LoadRoles(force bool) (err error) {
	if !(force || time.Since(p.lastLoadedRoles) > (5*time.Second)) {
		return
	}

	if g.PathExists(p.RolesFile) {
		var roles RoleMap
		rolesB, _ := os.ReadFile(p.RolesFile)
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
			p.Roles[strings.ToLower(name)] = role
		}
		p.lastLoadedRoles = time.Now()
	}
	return
}

func (p *Project) GetRoleMap(roles []string) (rm RoleMap) {
	rm = RoleMap{}
	for _, rn := range roles {
		rn = strings.ToLower(rn)
		if role, ok := p.Roles[rn]; ok {
			rm[rn] = role
		}
	}
	return
}

func (p *Project) GetConnObject(connName, databaseName string) (connObj connection.Connection, err error) {
	mux.Lock()
	connName = strings.ToLower(connName)
	c, ok := p.Connections[connName]
	mux.Unlock()
	if !ok {
		err = g.Error("could not find conn %s", connName)
		return
	}

	if databaseName == "" {
		// default
		return c.Conn, nil
	}

	// create new connection with specific database
	data := g.M()
	for k, v := range c.Conn.Data {
		data[k] = v
	}
	if databaseName != "" {
		data["database"] = strings.ToLower(databaseName)
	}
	delete(data, "url")
	delete(data, "schema")
	connObj, err = connection.NewConnectionFromMap(g.M("name", c.Conn.Name, "data", data, "type", c.Conn.Type))
	if err != nil {
		err = g.Error(err, "could not load connection %s", c.Conn.Name)
		return
	}
	return
}

func (p *Project) LoadConnections(force bool) (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()

	if !(time.Since(p.lastLoadedConns).Seconds() > 2 || force) {
		return
	}

	p.Connections = map[string]*Connection{}

	var connEntries []connection.ConnEntry
	if p.ID == DefaultProjectID {
		// get all connections
		connEntries = connection.GetLocalConns(force)
	} else {
		// need to load for the project only
		m := g.M()
		g.JSONConvert(env.LoadEnvFile(p.EnvFile), &m)
		profileConns, err := connection.ReadConnections(m)
		if err != nil {
			return g.Error(err, "could not read project env connections")
		}

		for name, pConn := range profileConns {
			entry := connection.ConnEntry{
				Name:       name,
				Connection: pConn,
				Source:     "project env yaml",
			}
			connEntries = append(connEntries, entry)
		}
	}

	for _, entry := range connEntries {
		if !entry.Connection.Type.IsDb() {
			continue
		}

		name := strings.ToLower(strings.ReplaceAll(entry.Name, "/", "_"))
		p.Connections[name] = &Connection{
			Conn:   entry.Connection,
			Source: entry.Source,
			Props:  map[string]string{},
		}
	}

	p.lastLoadedConns = time.Now()

	return nil
}

// SchemaAll returns schema.*
// notation for all tables in a schema
func (p *Project) SchemaAll(connection, schema string) (table database.Table) {
	conn, err := p.GetConnObject(connection, "")
	if err != nil {
		return database.Table{}
	}
	table, _ = database.ParseTableName(schema+".*", conn.Type)
	return table
}

func (p *Project) GetConnInstance(connName, databaseName string) (conn database.Connection, err error) {
	err = p.LoadConnections(false)
	if err != nil {
		err = g.Error(err, "could not load connections")
		return
	}

	mux.Lock()
	connName = strings.ToLower(connName)
	c := p.Connections[connName]
	mux.Unlock()

	connObj, err := p.GetConnObject(connName, databaseName)
	if err != nil {
		err = g.Error(err, "could not load connection %s", connName)
		return
	}

	// connect or use pool
	os.Setenv("USE_POOL", "TRUE")

	// init connection
	conn, err = connObj.AsDatabase(true)
	if err != nil {
		err = g.Error(err, "could not initialize database connection '%s' / '%s' with provided credentials/url.", connName, databaseName)
		return
	}

	err = conn.Connect()
	if err != nil {
		err = g.Error(err, "could not connect with provided credentials/url")
		return
	}
	c.Props = conn.Props()

	// set SetMaxIdleConns
	// conn.Db().SetMaxIdleConns(2)

	return
}

func (p *Project) NewQuery(ctx context.Context) *Query {
	q := new(Query)
	q.Project = p.ID
	q.Affected = -1
	q.lastTouch = time.Now()
	q.Context = g.NewContext(ctx)
	q.Done = make(chan struct{})
	return q
}
