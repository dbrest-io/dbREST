package server

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

func postConnectionSQL(c echo.Context) (err error) {

	req := NewRequest(c)

	// read query text
	body, _ := io.ReadAll(c.Request().Body)
	req.Query = string(body)

	if !req.Roles.CanSQL(req.Connection) {
		return g.ErrJSON(http.StatusForbidden, g.Error("Not allowed to submit custom SQL"))
	}

	return processQueryRequest(req)
}

func postConnectionCancel(c echo.Context) (err error) {

	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckID); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	} else if !req.Roles.CanSQL(req.Connection) {
		return g.ErrJSON(http.StatusForbidden, g.Error("Not allowed to cancel"))
	}

	query := state.NewQuery(context.Background())
	query.Conn = req.Connection
	query.ID = req.ID
	req.echoCtx.Set("query", query)

	err = query.Cancel()
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not cancel query")
	}

	return resp.Make()
}

func processQueryRequest(req Request) (err error) {
	// default ID if not provided
	req.ID = lo.Ternary(req.ID == "", g.NewTsID("sql"), req.ID)

	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckQuery); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	query := state.NewQuery(context.Background())
	query.Conn = req.Connection
	query.Database = req.Database
	query.Text = req.Query
	query.ID = req.ID

	query.Limit = cast.ToInt(req.echoCtx.QueryParam("limit"))
	if query.Limit == 0 {
		query.Limit = 500
	}

	cont := req.Header.Get("X-Request-Continue") != ""
	query, err = state.SubmitOrGetQuery(query, cont)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get process query")
	}
	req.echoCtx.Set("query", query)

	ticker := time.NewTicker(90 * time.Second)
	defer ticker.Stop()

	select {
	case <-query.Done:
		resp.ds = query.Stream
		err = query.ProcessResult()
		resp.Header.Set("X-Request-Status", string(query.Status))
		if err != nil {
			g.LogError(err)
			result := g.ToMap(query)
			result["err"] = g.ErrMsgSimple(err)
			resp.Payload = result
			return resp.Make()
		} else if query.Affected != -1 {
			resp.Payload = g.M("affected", query.Affected)
			return resp.Make()
		}
		return resp.MakeStreaming()
	case <-ticker.C:
		resp.Status = 202 // when status is 202, follow request with header "X-Request-Continue"
		resp.Payload = g.ToMap(query)
		resp.ec.Response().Header().Set("X-Request-Status", string(query.Status))
	}

	return resp.Make()
}
