package state

import (
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
)

var (
	mux sync.Mutex

	// set a build time.
	RudderstackURL = ""
)

func init() {
	// routine loop
	go loop()
}

// Connection is a connection
type Connection struct {
	Conn   connection.Connection
	Source string
	Props  map[string]string // to cache vars
}

// DefaultDB returns the default database
func (c *Connection) DefaultDB() string {
	return c.Conn.Info().Database
}

// GetConnInstance gets the connection instance
func CloseConnections() {
	mux.Lock()
	for _, p := range Projects {
		p.mux.Lock()
		for k, c := range p.Connections {
			g.LogError(c.Conn.Close())
			delete(p.Connections, k)
		}
		p.mux.Unlock()
	}
	mux.Unlock()
}

func ClearOldQueries() {
	mux.Lock()
	for _, p := range Projects {
		p.mux.Lock()
		for k, q := range p.Queries {
			if time.Since(q.lastTouch) > 10*time.Minute {
				delete(p.Queries, k)
			}
		}
		p.mux.Unlock()
	}
	mux.Unlock()
}

func loop() {
	ticker1Min := time.NewTicker(1 * time.Minute)
	defer ticker1Min.Stop()
	ticker10Min := time.NewTicker(10 * time.Minute)
	defer ticker10Min.Stop()

	for {
		select {
		case <-ticker1Min.C:
		case <-ticker10Min.C:
			go ClearOldQueries()
		}
	}
}
