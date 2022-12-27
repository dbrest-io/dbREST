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
	resp := NewResponse(req)

	// read query text
	body, _ := io.ReadAll(c.Request().Body)
	req.Query = string(body)

	// default ID if not provided
	req.ID = lo.Ternary(req.ID == "", g.NewTsID("sql"), req.ID)

	if err = req.Validate(reqCheckConnection, reqCheckQuery); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	query := state.NewQuery(context.Background())
	query.Conn = req.Connection
	query.Database = req.Database
	query.Text = req.Query
	query.ID = req.ID

	query.Limit = cast.ToInt(c.QueryParam("limit"))
	if query.Limit == 0 {
		query.Limit = 500
	}

	cont := c.Request().Header.Get("dbREST-Continue") != ""
	query, err = state.SubmitOrGetQuery(query, cont)
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not get process query")
	}

	ticker := time.NewTicker(90 * time.Second)
	defer ticker.Stop()

	select {
	case <-query.Done:
		resp.ds = query.Stream
		err = query.ProcessResult()
		resp.ec.Response().Header().Set("dbREST-Query-Status", string(query.Status))
		if err != nil {
			g.LogError(err)
			result := g.ToMap(query)
			result["error"] = g.ErrMsgSimple(err)
			resp.Payload = result
			return resp.Make()
		}
		return resp.MakeStreaming()
	case <-ticker.C:
		resp.Status = 202 // when status is 202, follow request with header "DbNet-Continue"
		resp.Payload = g.ToMap(query)
		resp.ec.Response().Header().Set("dbREST-Query-Status", string(query.Status))
	}

	return resp.Make()
}

func postConnectionCancel(c echo.Context) (err error) {

	req := NewRequest(c)
	resp := NewResponse(req)

	if err = req.Validate(reqCheckConnection, reqCheckID); err != nil {
		return ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	query := state.NewQuery(context.Background())
	query.Conn = req.Connection
	query.ID = req.ID

	err = query.Cancel()
	if err != nil {
		return ErrJSON(http.StatusInternalServerError, err, "could not cancel query")
	}

	return resp.Make()
}
