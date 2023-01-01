package server

import (
	"io"
	"net/http"
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
)

type Response struct {
	Request Request         `json:"-"`
	Error   string          `json:"error,omitempty"`
	Payload map[string]any  `json:"-"`
	Status  int             `json:"-"`
	ds      *iop.Datastream `json:"-"`
	data    iop.Dataset     `json:"-"`
	ec      echo.Context    `json:"-" query:"-"`
	Header  http.Header     `json:"-" query:"-"`
}

func NewResponse(req Request) Response {
	return Response{
		Request: req,
		ec:      req.echoCtx,
		Status:  200,
		Header:  req.echoCtx.Response().Header(),
	}
}

func (r *Response) MakeStreaming() (err error) {
	if r.Request.ID != "" {
		r.Header.Set("dbREST-Request-ID", r.Request.ID)
	}

	if r.ds == nil {
		return r.ec.String(http.StatusOK, "")
	}

	r.setHeaderColumns(r.ds.Columns)
	////////////////////

	respW := r.ec.Response().Writer
	var pushRow func(row []interface{})

	fields := r.ds.Columns.Names()
	acceptType := strings.ToLower(r.ec.Request().Header.Get(echo.HeaderAccept))

	switch acceptType {
	case "text/plain":
		r.Header.Set("Content-Type", "text/plain")
		csvW := csv.NewWriter(respW)
		csvW.Comma = '\t' // TSV

		// write headers
		csvW.Write(fields)
		csvW.Flush()

		pushRow = func(row []interface{}) {
			_, err = csvW.Write(r.ds.CastRowToString(row))
			if err != nil {
				r.ds.Context.Cancel()
				g.LogError(g.Error(err, "could not encode csv row"))
			}
			csvW.Flush()
		}
	case "text/csv":
		r.Header.Set("Content-Type", "text/csv")
		csvW := csv.NewWriter(respW)

		// write headers
		csvW.Write(fields)
		csvW.Flush()

		pushRow = func(row []interface{}) {
			_, err = csvW.Write(r.ds.CastRowToString(row))
			if err != nil {
				r.ds.Context.Cancel()
				g.LogError(g.Error(err, "could not encode csv row"))
			}
			csvW.Flush()
		}

	case "application/json":
		r.Header.Set("Content-Type", "application/json")
		data, err := r.ds.Collect(0)
		if err != nil {
			r.ds.Context.Cancel()
			g.LogError(g.Error(err, "could not encode json payload"))
		}
		out, _ := g.JSONMarshal(data.Records())
		respW.Write(out)
	default:
		r.Header.Set("Content-Type", "application/jsonlines")

		enc := json.NewEncoder(respW)
		pushRow = func(row []interface{}) {
			err := enc.Encode(row)
			if err != nil {
				r.ds.Context.Cancel()
				g.LogError(g.Error(err, "could not encode json record"))
			}
		}
		columnsI := lo.Map(r.ds.Columns, func(c iop.Column, i int) any {
			return c.Name
		})
		pushRow(columnsI) // first row is columns
	}

	// write headers
	r.Header.Set("Transfer-Encoding", "chunked")
	r.ec.Response().WriteHeader(r.Status)
	r.ec.Response().Flush()

	ctx := r.ec.Request().Context()
	for row := range r.ds.Rows {

		select {
		case <-ctx.Done():
			r.ds.Context.Cancel()
			if err = r.ds.Err(); err != nil {
				return ErrJSON(http.StatusInternalServerError, err)
			}
			return
		default:
			pushRow(row)
			r.ec.Response().Flush()
		}
	}

	return
}

func (r *Response) Make() (err error) {
	if r.Request.ID != "" {
		r.Header.Set("dbREST-Request-ID", r.Request.ID)
	}

	if r.Payload != nil {
		return r.ec.JSON(r.Status, r.Payload)
	}

	out := ""
	acceptType := r.ec.Request().Header.Get(echo.HeaderAccept)
	switch strings.ToLower(acceptType) {
	case "text/plain", "text/csv":
		r.Header.Set("Content-Type", "text/csv")
		if r.ds != nil {
			reader := r.ds.NewCsvReader(0, 0)
			b, _ := io.ReadAll(reader)
			out = string(b)
		} else if len(r.data.Columns) > 0 {
			r.setHeaderColumns(r.data.Columns)
			reader := r.data.Stream().NewCsvReader(0, 0)
			b, _ := io.ReadAll(reader)
			out = string(b)
		}
	case "application/json":
		r.Header.Set("Content-Type", "application/json")
		if r.ds != nil {
			data, _ := r.ds.Collect(0)
			r.data = data
		}

		if len(r.data.Columns) > 0 {
			r.setHeaderColumns(r.data.Columns)
			out = g.Marshal(r.data.Records())
		}
	default:
		r.Header.Set("Content-Type", "application/jsonlines")
		if r.ds != nil {
			data, _ := r.ds.Collect(0)
			r.data = data
		}

		if len(r.data.Columns) > 0 {
			r.setHeaderColumns(r.data.Columns)
			lines := []string{g.Marshal(r.data.Columns.Names())}
			for _, row := range r.data.Rows {
				lines = append(lines, g.Marshal(row))
			}
			out = strings.Join(lines, "\n")
		}
	}
	return r.ec.String(r.Status, out)
}

func (r Response) setHeaderColumns(cols iop.Columns) {
	columnsS := lo.Map(cols, func(c iop.Column, i int) any {
		return []string{c.Name, string(c.Type), c.DbType}
	})
	r.Header.Set("dbREST-Request-Columns", g.Marshal(columnsS))
}
