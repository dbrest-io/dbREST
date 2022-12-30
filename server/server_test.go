package server

import (
	"strings"
	"testing"
	"time"

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
	testConn   = "POSTGRES"
	testSchema = ""
	testTable  = ""
	testID     = "12345"
)

func TestServer(t *testing.T) {
	s := NewServer()
	s.Port = "1456"
	go s.Start()

	time.Sleep(time.Second)

	missingTests := []string{}
	for _, route := range standardRoutes {
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
		case "getConnections", "getConnectionDatabases", "getConnectionSchemas", "getConnectionTables", "getConnectionColumns", "getSchemaTables", "getSchemaColumns", "getTableColumns", "getTableSelect":
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
				sql := strings.NewReader("select pg_sleep(2) as a")
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
		default:
			missingTests = append(missingTests, route.Name)
		}
	}

	if len(missingTests) > 0 {
		g.Warn("No test for routes: %s", strings.Join(missingTests, ", "))
	}
}
