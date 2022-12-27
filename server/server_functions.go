package server

import (
	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
)

// ErrJSON returns to the echo.Context as JSON formatted
func ErrJSON(HTTPStatus int, err error, args ...interface{}) error {
	msg := g.ArgsErrMsg(args...)
	g.LogError(err)
	if msg == "" {
		msg = g.ErrMsg(err)
	} else if g.ErrMsg(err) != "" {
		msg = g.F("%s [%s]", msg, g.ErrMsg(err))
	}
	return echo.NewHTTPError(HTTPStatus, g.M("error", msg))
}
