package server

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/dbio/database"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
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
	randomRow  = func() (rec map[string]any) { return }
)

func TestServer(t *testing.T) {
	deleteTestDB()
	defer deleteTestDB()
	err := createTestDB()
	if !assert.NoError(t, err) {
		return
	}

	s := NewServer()
	s.Port = "1456"
	go s.Start()
	defer s.Close()

	time.Sleep(time.Second)

	missingTests := []string{}
	for _, route := range standardRoutes {
		if t.Failed() {
			break
		}

		g.Info("Testing route: %s", route.Name)

		respMap := map[string]any{}
		respArr := []map[string]any{}

		url := g.F("%s%s", s.Hostname(), route.Path)
		url = strings.ReplaceAll(url, ":connection", testConn)
		url = strings.ReplaceAll(url, ":schema", testSchema)
		url = strings.ReplaceAll(url, ":table", testTable)
		url = strings.ReplaceAll(url, ":id", testID)

		msg := g.F("%s => %s %s", route.Name, route.Method, url)

		switch route.Name {
		case "getStatus":
			resp, respBytes, err := net.ClientDo(route.Method, url, nil, headers)
			assert.NoError(t, err, msg)
			assert.Less(t, resp.StatusCode, 300, msg)
			assert.Equal(t, "OK", string(respBytes), msg)
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
