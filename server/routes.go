package server

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

var standardRoutes = []echo.Route{
	{
		Method: "GET",
		Path:   "/",
		Handler: func(c echo.Context) error {
			return c.String(http.StatusOK, "Hello world!")
		},
	},
}
