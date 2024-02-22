package server

import (
	"net/http"
	"strings"

	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

func getTableColumns(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	resp := NewResponse(req)

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		tableColumns, err := c.GetTableColumns(&req.dbTable)
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
		if req.CanRead(req.dbTable) || req.CanWrite(req.dbTable) {
			for _, column := range tableColumns {
				row := []any{
					req.Database,
					req.Schema,
					req.Table,
					nil,
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

	return resp.Make()
}

func getTableSelect(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	} else if !req.CanRead(req.dbTable) {
		return g.ErrJSON(http.StatusForbidden, g.Error("Not allowed"))
	}

	// construct SQL Query
	{
		conn, err := req.Project.GetConnObject(req.Connection, "")
		if err != nil {
			err = ErrJSON(http.StatusNotFound, err, "could not find connection: %s", req.Connection)
			return err
		}

		var preOptions, postOptions string

		// TODO: parse fields to ensure no SQL injection
		var fields []string
		var limit int
		whereMap := map[string]string{}

		for k, v := range req.echoCtx.QueryParams() {
			switch k {
			case ".columns":
				fields = strings.Split(v[0], ",")
			case ".limit":
				limit = cast.ToInt(v[0])
				limit = lo.Ternary(limit == 0, 100, limit)
			default:
				whereMap[k] = v[0]
			}
		}

		makeWhere := func() (ws string) {
			arr := []string{}
			for k, v := range whereMap {
				expr := g.F("%s=%s", k, v)
				arr = append(arr, expr)
			}
			// TODO: SQL Injection is possible, need to use bind vars
			return strings.Join(arr, " and ")
		}

		if limit > 0 { // For unlimited, specify -1
			switch conn.Type {
			case dbio.TypeDbSQLServer:
				preOptions = preOptions + g.F("top %d", limit)
			default:
				postOptions = postOptions + g.F("limit %d", limit)
			}

			// set for processQueryRequest
			req.echoCtx.QueryParams().Set("limit", cast.ToString(limit))
		}

		noFields := len(fields) == 0 || (len(fields) == 1 && fields[0] == "")
		noWhere := len(whereMap) == 0

		req.Query = g.R(
			"select{preOptions} {fields} from {table} where {where} {postOptions}",
			"fields", lo.Ternary(noFields, "*", strings.Join(fields, ", ")),
			"table", req.dbTable.FullName(),
			"where", lo.Ternary(noWhere, "1=1", makeWhere()),
			"preOptions", lo.Ternary(preOptions != "", " "+preOptions, ""),
			"postOptions", lo.Ternary(postOptions != "", " "+postOptions, ""),
		)
	}

	return processQueryRequest(req)
}

func getTableIndexes(c echo.Context) (err error) {

	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		return c.GetIndexes(req.dbTable.FullName())
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		err = ErrJSON(http.StatusBadRequest, err, "could not get table indexes")
		return
	}

	return resp.Make()
}

func getTableKeys(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		return c.GetPrimaryKeys(req.dbTable.FullName())
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		err = ErrJSON(http.StatusBadRequest, err, "could not get table keys")
		return
	}

	return resp.Make()
}

func postTableInsert(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	} else if !req.CanWrite(req.dbTable) {
		return g.ErrJSON(http.StatusForbidden, g.Error("Not allowed"))
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {

		bulk := req.echoCtx.QueryParam(".bulk")

		ds, err := req.GetDatastream()
		if err != nil {
			err = g.Error(err, "could not get datastream")
			return
		}

		if req.echoCtx.QueryParam("bulk") == "true" {
			_ = bulk
			// TODO: bulk loading option
			// df, err := iop.MakeDataFlow(ds.Split()...)
			// if err != nil {
			// 	err = g.Error(err, "could not make dataflow")
			// 	return
			// }
		}

		ctx := req.echoCtx.Request().Context()
		err = c.BeginContext(ctx)
		if err != nil {
			err = g.Error(err, "could not begin transaction")
			return
		}

		count, err := c.InsertBatchStream(req.dbTable.FullName(), ds)
		if err != nil {
			err = g.Error(err, "could not insert into table")
			return
		}

		err = c.Commit()
		if err != nil {
			err = g.Error(err, "could not commit transaction")
			return
		}

		resp.Payload = g.M("affected", count)

		return
	}

	_, err = ProcessRequest(req, rf)
	if err != nil {
		err = ErrJSON(http.StatusBadRequest, err, "could not get process request")
		return
	}

	return resp.Make()
}

func postTableUpsert(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)
	resp.Status = http.StatusNotImplemented
	resp.Payload = g.M("error", "Not-Implemented")
	return resp.Make()

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	} else if !req.CanWrite(req.dbTable) {
		return g.ErrJSON(http.StatusForbidden, g.Error("Not allowed"))
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {

		bulk := req.echoCtx.QueryParam("bulk")

		ds, err := req.GetDatastream()
		if err != nil {
			err = g.Error(err, "could not get datastream")
			return
		}

		if req.echoCtx.QueryParam("bulk") == "true" {
			_ = bulk
			// TODO: bulk loading option
			// df, err := iop.MakeDataFlow(ds.Split()...)
			// if err != nil {
			// 	err = g.Error(err, "could not make dataflow")
			// 	return
			// }
		}

		ctx := req.echoCtx.Request().Context()
		err = c.BeginContext(ctx)
		if err != nil {
			err = g.Error(err, "could not begin transaction")
			return
		}

		// TODO: add c.UpsertBatchStream
		count, err := c.InsertBatchStream(req.dbTable.FullName(), ds)
		if err != nil {
			err = g.Error(err, "could not insert into table")
			return
		}

		err = c.Commit()
		if err != nil {
			err = g.Error(err, "could not commit transaction")
			return
		}

		resp.Payload = g.M("affected", count)

		return
	}

	_, err = ProcessRequest(req, rf)
	if err != nil {
		err = ErrJSON(http.StatusBadRequest, err, "could not get process request")
		return
	}

	return resp.Make()
}

func patchTableUpdate(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)
	resp.Status = http.StatusNotImplemented
	resp.Payload = g.M("error", "Not-Implemented")
	return resp.Make()

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	} else if !req.CanWrite(req.dbTable) {
		return g.ErrJSON(http.StatusForbidden, g.Error("Not allowed"))
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		bulk := req.echoCtx.QueryParam("bulk")

		ds, err := req.GetDatastream()
		if err != nil {
			err = g.Error(err, "could not get datastream")
			return
		}

		if req.echoCtx.QueryParam("bulk") == "true" {
			_ = bulk
			_ = ds
			// TODO: bulk loading option
			// df, err := iop.MakeDataFlow(ds.Split()...)
			// if err != nil {
			// 	err = g.Error(err, "could not make dataflow")
			// 	return
			// }
		}

		ctx := req.echoCtx.Request().Context()
		err = c.BeginContext(ctx)
		if err != nil {
			err = g.Error(err, "could not begin transaction")
			return
		}

		// TODO: add c.UpdateBatchStream
		count, err := c.InsertBatchStream(req.dbTable.FullName(), ds)
		if err != nil {
			err = g.Error(err, "could not insert into table")
			return
		}

		err = c.Commit()
		if err != nil {
			err = g.Error(err, "could not commit transaction")
			return
		}

		data.Columns = iop.Columns{{Name: "affected", Type: iop.IntegerType}}
		data.Append([]any{count})

		return
	}

	resp.data, err = ProcessRequest(req, rf)
	if err != nil {
		err = ErrJSON(http.StatusBadRequest, err, "could not get process request")
		return
	}

	return resp.Make()
}
