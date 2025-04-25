package server

import (
	"net/http"
	"strings"

	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

func getConnections(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	// load fresh connections
	req.Project.LoadConnections(true)

	columns := iop.Columns{
		{Name: "name", Type: iop.StringType},
		{Name: "type", Type: iop.StringType},
		{Name: "database", Type: iop.StringType},
		{Name: "source", Type: iop.StringType},
	}
	resp.data = iop.NewDataset(columns)
	for _, conn := range req.Project.Connections {
		connName := strings.ToLower(conn.Conn.Info().Name)
		if !req.Roles.HasAccess(connName) {
			continue
		}

		row := []any{
			connName,
			conn.Conn.Info().Type,
			conn.Conn.Info().Database,
			conn.Source,
		}
		resp.data.Append(row)
	}

	resp.data.Sort(0, true)

	return resp.Make()
}

func closeConnection(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		err = c.Close()
		return
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not close connection")
	}

	// for middleware
	req.echoCtx.Set("data", &resp.data)

	return resp.Make()
}

func getConnectionDatabases(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		return c.GetDatabases()
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get databases")
	}

	resp.data.Sort(0, true)

	// for middleware
	req.echoCtx.Set("data", &resp.data)

	return resp.Make()
}

func getConnectionSchemas(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		data, err = c.GetSchemas()
		data.Rows = lo.Filter(data.Rows, func(row []any, i int) bool {
			schema := strings.ToLower(cast.ToString(row[0]))
			ts := req.Project.SchemaAll(req.Connection, schema)
			return req.CanRead(ts) || req.CanWrite(ts)
		})

		return
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get schemas")
	}

	resp.data.Sort(0, true)

	// for middleware
	req.echoCtx.Set("data", &resp.data)

	return resp.Make()
}

func getConnectionTables(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	resp, err := getSchemataTables(req)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get tables")
	}

	return resp.Make()
}

func getConnectionColumns(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	resp, err := getSchemataColumns(req)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get columns")
	}

	return resp.Make()
}

func getSchemaTables(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection, reqCheckSchema); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	resp, err := getSchemataTables(req)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get tables")
	}

	return resp.Make()
}

func getSchemaColumns(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection, reqCheckSchema); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	resp, err := getSchemataColumns(req)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get columns")
	}

	return resp.Make()
}

func getSchemataTables(req Request) (resp Response, err error) {
	resp = NewResponse(req)

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		schemata, err := c.GetSchemata(database.SchemataLevelTable, req.Schema)
		if err != nil {
			err = g.Error(err, "could not get tables")
			return
		}

		columns := iop.Columns{
			{Name: "database_name", Type: iop.StringType},
			{Name: "schema_name", Type: iop.StringType},
			{Name: "table_name", Type: iop.StringType},
			{Name: "table_type", Type: iop.StringType},
		}
		data = iop.NewDataset(columns)
		for _, table := range schemata.Tables() {
			if !(req.CanRead(table) || req.CanWrite(table)) {
				continue
			}
			row := []any{
				table.Database,
				table.Schema,
				table.Name,
				lo.Ternary(table.IsView, "view", "table"),
			}
			data.Append(row)
		}

		data.Sort(0, 1, 2)

		// for middleware
		req.echoCtx.Set("data", &data)

		return
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		err = g.Error(err, "could not get schemas")
		return
	}

	return resp, nil
}

func getSchemataColumns(req Request) (resp Response, err error) {

	resp = NewResponse(req)

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		schemata, err := c.GetSchemata(database.SchemataLevelColumn, req.Schema, req.Table)
		if err != nil {
			err = g.Error(err, "could not get columns")
			return
		}

		columns := iop.Columns{
			{Name: "database_name", Type: iop.StringType},
			{Name: "schema_name", Type: iop.StringType},
			{Name: "table_name", Type: iop.StringType},
			{Name: "table_type", Type: iop.StringType},
			{Name: "column_id", Type: iop.IntegerType},
			{Name: "column_name", Type: iop.BoolType},
			{Name: "column_type", Type: iop.BoolType},
		}
		data = iop.NewDataset(columns)
		for _, table := range schemata.Tables() {
			if !(req.CanRead(table) || req.CanWrite(table)) {
				continue
			}

			for _, column := range table.Columns {
				row := []any{
					table.Database,
					table.Schema,
					table.Name,
					lo.Ternary(table.IsView, "view", "table"),
					column.Position,
					column.Name,
					column.DbType,
				}
				data.Append(row)
			}
		}

		data.Sort(0, 1, 2, 4)

		// for middleware
		req.echoCtx.Set("data", &data)

		return
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		err = g.Error(err, "could not get columns")
		return
	}

	return resp, nil
}
