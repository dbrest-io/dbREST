package server

import (
	"net/http"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/g"
	jsoniter "github.com/json-iterator/go"
	"github.com/labstack/echo/v5"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var StandardRoutes = []echo.Route{
	{
		Name:    "getStatus",
		Method:  "GET",
		Path:    "/.status",
		Handler: getStatus,
	},
	{
		Name:    "getConnections",
		Method:  "GET",
		Path:    "/.connections",
		Handler: getConnections,
	},
	{
		Name:    "closeConnection",
		Method:  "POST",
		Path:    "/:connection/.close",
		Handler: closeConnection,
	},
	{
		Name:    "getConnectionDatabases",
		Method:  "GET",
		Path:    "/:connection/.databases",
		Handler: getConnectionDatabases,
	},
	{
		Name:    "getConnectionSchemas",
		Method:  "GET",
		Path:    "/:connection/.schemas",
		Handler: getConnectionSchemas,
	},
	{
		Name:    "getConnectionTables",
		Method:  "GET",
		Path:    "/:connection/.tables",
		Handler: getConnectionTables,
	},
	{
		Name:    "getConnectionColumns",
		Method:  "GET",
		Path:    "/:connection/.columns",
		Handler: getConnectionColumns,
	},
	{
		Name:    "submitSQL",
		Method:  "POST",
		Path:    "/:connection/.sql",
		Handler: postConnectionSQL,
	},
	{
		Name:    "submitSQL_ID",
		Method:  "POST",
		Path:    "/:connection/.sql/:id",
		Handler: postConnectionSQL,
	},
	{
		Name:    "cancelSQL",
		Method:  "POST",
		Path:    "/:connection/.cancel/:id",
		Handler: postConnectionCancel,
	},
	{
		Name:    "getSchemaTables",
		Method:  "GET",
		Path:    "/:connection/:schema/.tables",
		Handler: getSchemaTables,
	},
	{
		Name:    "getSchemaColumns",
		Method:  "GET",
		Path:    "/:connection/:schema/.columns",
		Handler: getSchemaColumns,
	},
	{
		Name:    "getTableColumns",
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.columns",
		Handler: getTableColumns,
	},
	{
		Name:    "getTableIndexes",
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.indexes",
		Handler: getTableIndexes,
	},
	{
		Name:    "getTableKeys",
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.keys",
		Handler: getTableKeys,
	},
	{
		Name:    "tableInsert",
		Method:  "POST",
		Path:    "/:connection/:schema/:table",
		Handler: postTableInsert,
	},
	{
		Name:    "tableUpsert",
		Method:  "PUT",
		Path:    "/:connection/:schema/:table",
		Handler: postTableUpsert,
	},
	{
		Name:    "tableUpdate",
		Method:  "PATCH",
		Path:    "/:connection/:schema/:table",
		Handler: patchTableUpdate,
	},
	{
		Name:    "getTableSelect",
		Method:  "GET",
		Path:    "/:connection/:schema/:table",
		Handler: getTableSelect,
	},
}

func getStatus(c echo.Context) (err error) {
	out := g.F("dbREST %s", state.Version)
	return c.String(http.StatusOK, out)
}
