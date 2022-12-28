package server

import (
	"net/http"
	"strings"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/dbio"
	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

func getTableColumns(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	resp, err := getSchemataColumns(req)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get columns")
	}

	return resp.Make()
}

func getTableSelect(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	// construct SQL Query
	{
		conn, err := state.GetConnObject(req.Connection, "")
		if err != nil {
			err = ErrJSON(http.StatusNotFound, err, "could not find connection: %s", req.Connection)
			return err
		}

		var preOptions, postOptions string

		// TODO: parse fields to ensure no SQL injection
		fields := strings.Split(req.echoCtx.QueryParam("fields"), ",")
		limit := cast.ToInt(req.echoCtx.QueryParam("limit"))
		limit = lo.Ternary(limit == 0, 100, limit) // default to 100

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

		req.Query = g.R(
			"select{preOptions} {fields} from {table}{postOptions}",
			"fields", lo.Ternary(noFields, "*", strings.Join(fields, ", ")),
			"table", req.dbTable.FullName(),
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

func postTableInsertUpsert(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		return

		bulk := req.echoCtx.QueryParam("bulk")
		strategy := req.echoCtx.QueryParam("strategy")

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

		var count uint64
		if strategy == "upsert" {
			// TODO: add c.UpsertBatchStream
		} else {
			count, err = c.InsertBatchStream(req.dbTable.FullName(), ds)
			if err != nil {
				err = g.Error(err, "could not insert into table")
				return
			}
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

func patchTableUpdate(c echo.Context) (err error) {
	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	rf := func(c database.Connection, req Request) (data iop.Dataset, err error) {
		return
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

		var count uint64
		// TODO: add c.UpdateBatchStream

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
