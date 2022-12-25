package server

import (
	"net/http"

	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
)

func getTableColumns(c echo.Context) (err error) {
	req := NewRequest(c)

	if err = req.Validate(reqCheckConnection, reqCheckSchema, reqCheckTable); err != nil {
		return g.ErrJSON(http.StatusBadRequest, err, "invalid request")
	}

	resp, err := getSchemataColumns(req)
	if err != nil {
		return g.ErrJSON(http.StatusInternalServerError, err, "could not get columns")
	}

	return resp.Make()
}

func getTableIndexes(c echo.Context) (err error)       { return }
func getTableKeys(c echo.Context) (err error)          { return }
func getTableSelect(c echo.Context) (err error)        { return }
func postTableInsertUpsert(c echo.Context) (err error) { return }
func patchTableUpdate(c echo.Context) (err error)      { return }
