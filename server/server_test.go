package server

import (
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/dbrest-io/dbrest/env"
	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/dbio/database"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/labstack/echo/v5"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

/*
roles
token
Routes
*/

var (
	headers = map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
	}
	testConn   = "SQLITE_TEST"
	testSchema = ""
	testTable  = ""
	testID     = "12345"
	tokenRW    = ""
	tokenR     = ""
	tokenW     = ""
	randomRow  = func() (rec map[string]any) { return }
)

func TestServer(t *testing.T) {
	// init
	deleteTestDB()
	defer deleteTestDB()
	err := createTestDB()
	if !assert.NoError(t, err) {
		return
	}

	// set roles & tokens
	setTestRoles()
	setTestToken()
	headers["Authorization"] = tokenRW

	s := NewServer()
	s.Port = "1456"
	go s.Start()
	defer s.Close()

	time.Sleep(time.Second)

	makeURL := func(route echo.Route) string {
		url := g.F("%s%s", s.Hostname(), route.Path)
		url = strings.ReplaceAll(url, ":connection", testConn)
		url = strings.ReplaceAll(url, ":schema", testSchema)
		url = strings.ReplaceAll(url, ":table", testTable)
		url = strings.ReplaceAll(url, ":id", testID)
		return url
	}

	// Test RW
	missingTests := []string{}
	for _, route := range StandardRoutes {
		if t.Failed() {
			break
		}

		g.Info("Testing route: %s with TokenRW", route.Name)

		respMap := map[string]any{}
		respArr := []map[string]any{}

		url := makeURL(route)

		msg := g.F("%s => %s %s", route.Name, route.Method, url)

		switch route.Name {
		case "getStatus":
			resp, respBytes, err := net.ClientDo(route.Method, url, nil, headers)
			assert.NoError(t, err, msg)
			assert.Less(t, resp.StatusCode, 300, msg)
			assert.Equal(t, "dbREST dev", string(respBytes), msg)
		case "getConnections", "getConnectionDatabases", "getConnectionSchemas", "getConnectionTables", "getConnectionColumns", "getSchemaTables", "getSchemaColumns", "getTableColumns", "getTableSelect", "getTableKeys":
			resp, respBytes, err := net.ClientDo(route.Method, url, nil, headers)
			g.Unmarshal(string(respBytes), &respArr)
			assert.NoError(t, err, msg)
			assert.Less(t, resp.StatusCode, 300, msg)
			assert.Greater(t, len(respArr), 0, msg)
			g.Debug("   got %d rows", len(respArr))

			if route.Name == "getConnectionTables" && len(respArr) > 0 {
				for _, row := range respArr {
					testSchema = cast.ToString(row["schema_name"])
					testTable = cast.ToString(row["table_name"])
					if !g.In(testSchema, "information_schema", "pg_catalog") {
						break // pick a good test table
					}
				}
			}
		case "submitSQL_ID":
			// delay so we can cancel
			go func() {
				sql := strings.NewReader(longQuery)
				resp, respBytes, err := net.ClientDo(route.Method, url, sql, headers)
				g.Unmarshal(string(respBytes), &respMap)
				assert.NoError(t, err, msg)
				assert.Less(t, resp.StatusCode, 300, msg)
				assert.NotEmpty(t, respMap["error"], msg)
			}()
			time.Sleep(100 * time.Millisecond)
		case "cancelSQL":
			// cancel delayed query
			resp, respBytes, err := net.ClientDo(route.Method, url, nil, headers)
			g.Unmarshal(string(respBytes), &respArr)
			assert.NoError(t, err, msg)
			assert.Less(t, resp.StatusCode, 300, msg)
		case "submitSQL":
			sql := strings.NewReader("select 1 as a, 2 as b")
			resp, respBytes, err := net.ClientDo(route.Method, url, sql, headers)
			g.Unmarshal(string(respBytes), &respArr)
			assert.NoError(t, err, msg)
			assert.Less(t, resp.StatusCode, 300, msg)
			assert.Greater(t, len(respArr), 0, msg)
			g.Debug("   got %d rows", len(respArr))
		case "tableInsert":
			recs := []map[string]any{}
			for i := 0; i < 10; i++ {
				recs = append(recs, randomRow())
			}
			payload := strings.NewReader(g.Marshal(recs))
			resp, respBytes, err := net.ClientDo(route.Method, url, payload, headers)
			g.Unmarshal(string(respBytes), &respMap)
			assert.NoError(t, err, msg)
			assert.Less(t, resp.StatusCode, 300, msg)
			assert.EqualValues(t, respMap["affected"], len(recs))
		default:
			missingTests = append(missingTests, route.Name)
		}
	}

	if len(missingTests) > 0 {
		g.Warn("No test for routes: %s", strings.Join(missingTests, ", "))
	}

	// Test R
	headers["Authorization"] = tokenR
	for _, route := range StandardRoutes {
		if t.Failed() {
			break
		} else if !g.In(route.Name, "getTableSelect", "tableInsert", "submitSQL") {
			continue
		}

		g.Info("Testing route: %s with TokenR", route.Name)

		url := makeURL(route)
		msg := g.F("%s => %s %s", route.Name, route.Method, url)

		switch route.Name {
		case "getTableSelect":
			// we should have access to place
			testTable = "place"
			url = makeURL(route)
			_, _, err = net.ClientDo(route.Method, url, nil, headers)
			assert.NoError(t, err, msg)

			// we should not have access to place2
			testTable = "place2"
			url = makeURL(route)
			_, _, err = net.ClientDo(route.Method, url, nil, headers)
			assert.Error(t, err, msg)
		case "tableInsert":
			// we should not have write access to any tables
			testTable = "place2"
			url = makeURL(route)
			recs := []map[string]any{}
			for i := 0; i < 10; i++ {
				recs = append(recs, randomRow())
			}
			payload := strings.NewReader(g.Marshal(recs))
			_, _, err = net.ClientDo(route.Method, url, payload, headers)
			assert.Error(t, err, msg)
		case "submitSQL":
			// we should not have sql access
			sql := strings.NewReader("select 1 as a, 2 as b")
			_, _, err := net.ClientDo(route.Method, url, sql, headers)
			assert.Error(t, err, msg)
		}
	}

	// Test W
	headers["Authorization"] = tokenW
	for _, route := range StandardRoutes {
		if t.Failed() {
			break
		} else if !g.In(route.Name, "getTableSelect", "tableInsert", "submitSQL") {
			continue
		}

		g.Info("Testing route: %s with TokenW", route.Name)

		url := makeURL(route)
		msg := g.F("%s => %s %s", route.Name, route.Method, url)

		switch route.Name {
		case "getTableSelect":
			// we should not have access to any table
			testTable = "place"
			url = makeURL(route)
			_, _, err = net.ClientDo(route.Method, url, nil, headers)
			assert.Error(t, err, msg)
		case "tableInsert":
			// we should have write access to place
			testTable = "place"
			url = makeURL(route)
			recs := []map[string]any{}
			for i := 0; i < 10; i++ {
				recs = append(recs, randomRow())
			}
			payload := strings.NewReader(g.Marshal(recs))
			_, _, err = net.ClientDo(route.Method, url, payload, headers)
			assert.NoError(t, err, msg)

			testTable = "place2"
			url = makeURL(route)
			payload = strings.NewReader(g.Marshal(recs))
			_, _, err = net.ClientDo(route.Method, url, payload, headers)
			assert.Error(t, err, msg)
		case "submitSQL":
			// we should not have sql access
			sql := strings.NewReader("select 1 as a, 2 as b")
			_, _, err := net.ClientDo(route.Method, url, sql, headers)
			assert.Error(t, err, msg)
		}
	}
}

var longQuery = `
-- https://dba.stackexchange.com/questions/203545/write-a-slow-sqlite-query-to-test-timeout
WITH RECURSIVE r(i) AS (
	VALUES(0)
	UNION ALL
	SELECT i FROM r
	LIMIT 1000000
)
SELECT i FROM r WHERE i = 1`

func createTestDB() (err error) {
	testDbURL := "sqlite://./test.db"
	conn, err := database.NewConn(testDbURL)
	if err != nil {
		return err
	}

	_, err = conn.Exec(`CREATE TABLE "place" ("id" int, "country" varchar(255), "city" varchar(255), "telcode" bigint, primary key (id))`)
	if err != nil {
		return err
	}

	_, err = conn.Exec(`CREATE INDEX idx_country_city ON place(country, city)`)
	if err != nil {
		return err
	}

	_, err = conn.Exec(`CREATE TABLE "place2" ("id" int, "country" varchar(255), "city" varchar(255), "telcode" bigint, primary key (id))`)
	if err != nil {
		return err
	}

	conn.Close()

	os.Setenv("SQLITE_TEST", testDbURL)
	state.LoadConnections(true)

	countries := []string{
		"Canada",
		"USA",
		"Brazil",
		"Russia",
		"India",
	}

	cities := []string{
		"Big City",
		"Small City",
		"Tiny City",
	}

	randomRow = func() (rec map[string]any) {
		return map[string]any{
			"id":      g.RandInt(10000),
			"country": countries[g.RandInt(5)],
			"city":    cities[g.RandInt(3)],
			"telcode": 100000000 + g.RandInt(900000000),
		}
	}

	return
}

func deleteTestDB() { os.Remove("./test.db") }

func setTestRoles() {
	testRoleRW := state.Role{}
	testRoleR := state.Role{}
	testRoleW := state.Role{}
	for connName := range state.Connections {
		connName = strings.ToLower(connName)
		testRoleRW[connName] = state.Grant{
			AllowRead:  []string{"*"},
			AllowWrite: []string{"*"},
			AllowSQL:   state.AllowSQLAny,
		}
		testRoleR[connName] = state.Grant{
			AllowRead:  []string{"main.place"},
			AllowWrite: []string{},
			AllowSQL:   state.AllowSQLDisable,
		}
		testRoleW[connName] = state.Grant{
			AllowRead:  []string{},
			AllowWrite: []string{"main.place"},
			AllowSQL:   state.AllowSQLDisable,
		}
	}
	state.Roles = state.RoleMap{
		"role_rw": testRoleRW,
		"role_r":  testRoleR,
		"role_w":  testRoleW,
	}
}

func setTestToken() {
	env.HomeDirTokenFile = path.Join(".tokens.test")
	token := state.NewToken([]string{"role_rw"})
	err := state.Tokens.Add("token_rw", token)
	g.LogFatal(err)
	tokenRW = token.Token

	token = state.NewToken([]string{"role_r"})
	err = state.Tokens.Add("token_r", token)
	g.LogFatal(err)
	tokenR = token.Token

	token = state.NewToken([]string{"role_w"})
	err = state.Tokens.Add("token_w", token)
	g.LogFatal(err)
	tokenW = token.Token
}
