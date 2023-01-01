package state

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/flarco/dbio/connection"
	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/env"
	"github.com/flarco/g"
)

var (
	// Connections is all connections
	Connections = map[string]*Connection{}
	Queries     = map[string]*Query{}
	Tokens      = TokenMap{}
	TokenValues = map[string]Token{}

	// Queries     = map[string]*store.Query{}
	// Jobs        = map[string]*store.Job{}
	// Sync syncs to store
	// Sync           = store.Sync
	HomeDir        = ""
	HomeDirEnvFile = ""
	mux            sync.Mutex
)

func init() {
	HomeDir = env.SetHomeDir("dbrest")
	HomeDirEnvFile = env.GetEnvFilePath(HomeDir)

	// create env file if not exists
	os.MkdirAll(HomeDir, 0755)
	if HomeDir != "" && !g.PathExists(HomeDirEnvFile) {
		defaultEnvBytes, _ := env.EnvFolder.ReadFile("default.env.yaml")
		defaultEnvBytes = append([]byte("# See https://docs.dbnet.io/\n"), defaultEnvBytes...)
		ioutil.WriteFile(HomeDirEnvFile, defaultEnvBytes, 0644)
	}

	// other sources of creds
	env.SetHomeDir("sling") // https://github.com/slingdata-io/sling
	env.SetHomeDir("dbnet") // https://github.com/dbnet-io/dbnet

	// load first time
	LoadConnections(true)
}

// Connection is a connection
type Connection struct {
	Conn  connection.Connection
	Props map[string]string // to cache vars
}

// DefaultDB returns the default database
func (c *Connection) DefaultDB() string {
	return c.Conn.Info().Database
}

var lastLoaded time.Time

func LoadConnections(force bool) (err error) {
	mux.Lock()
	defer mux.Unlock()

	if !(time.Since(lastLoaded).Seconds() > 2 || force) {
		return
	}

	Connections = map[string]*Connection{}

	connEntries := connection.GetLocalConns(force)
	for _, entry := range connEntries {
		if !entry.Connection.Type.IsDb() {
			continue
		}

		name := strings.ToUpper(strings.ReplaceAll(entry.Name, "/", "_"))
		Connections[name] = &Connection{
			Conn:  entry.Connection,
			Props: map[string]string{},
		}
	}

	lastLoaded = time.Now()

	return
}

// GetConnInstance gets the connection object
func GetConnObject(connName, databaseName string) (connObj connection.Connection, err error) {
	mux.Lock()
	connName = strings.ToUpper(connName)
	c, ok := Connections[connName]
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

// GetConnInstance gets the connection instance
func CloseConnections() {
	mux.Lock()
	for k, c := range Connections {
		g.LogError(c.Conn.Close())
		delete(Connections, k)
	}
	mux.Unlock()
}

// GetConnInstance gets the connection instance
func GetConnInstance(connName, databaseName string) (conn database.Connection, err error) {
	err = LoadConnections(false)
	if err != nil {
		err = g.Error(err, "could not load connections")
		return
	}

	mux.Lock()
	connName = strings.ToUpper(connName)
	c := Connections[connName]
	mux.Unlock()

	connObj, err := GetConnObject(connName, databaseName)
	if err != nil {
		err = g.Error(err, "could not load connection %s", connName)
		return
	}

	// connect or use pool
	os.Setenv("USE_POOL", "TRUE")

	// init connection
	props := append(g.MapToKVArr(c.Props), g.MapToKVArr(connObj.DataS())...)
	conn, err = database.NewConn(connObj.URL(), props...)
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
