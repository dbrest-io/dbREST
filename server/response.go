package server

import (
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
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
	resp := Response{
		Request: req,
		ec:      req.echoCtx,
		Status:  200,
		Header:  req.echoCtx.Response().Header(),
	}
	resp.Header.Set("X-Request-ID", req.ID)
	resp.Header.Set("Access-Control-Expose-Headers", "X-Request-ID, X-Request-Columns, X-Request-Status, X-Request-Continue, X-Project-ID")
	return resp
}

func (r *Response) MakeStreaming() (err error) {

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
		// convert all values to string since JS can truncate int values
		// above Number.MAX_SAFE_INTEGER
		out, _ := g.JSONMarshal(StringRecords(&data))
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
	for row := range r.ds.Rows() {

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

	if r.Payload != nil {
		r.Header.Set("Content-Type", "application/json")
		return r.ec.JSON(r.Status, r.Payload)
	}

	out := ""
	acceptType := r.ec.Request().Header.Get(echo.HeaderAccept)
	switch strings.ToLower(acceptType) {
	case "text/plain", "text/csv":
		r.Header.Set("Content-Type", "text/csv")
		if r.ds != nil {
			reader := r.ds.NewCsvReader(iop.DefaultStreamConfig())
			b, _ := io.ReadAll(reader)
			out = string(b)
		} else if len(r.data.Columns) > 0 {
			r.setHeaderColumns(r.data.Columns)
			reader := r.data.Stream().NewCsvReader(iop.DefaultStreamConfig())
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
			out = g.Marshal(r.data.Records(false))
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
	r.Header.Set("X-Request-Columns", g.Marshal(columnsS))
}

// Records return rows of maps or string values
func StringRecords(data *iop.Dataset) []map[string]interface{} {
	records := make([]map[string]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]interface{}{}
		for j, field := range data.GetFields(false) {
			switch v := row[j].(type) {
			case nil:
				rec[field] = nil
			case time.Time:
				rec[field] = strings.ReplaceAll(v.Format("2006-01-02 15:04:05.000000Z07"), " 00:00:00.000000Z", "")
			case *time.Time:
				rec[field] = strings.ReplaceAll(v.Format("2006-01-02 15:04:05.000000Z07"), " 00:00:00.000000Z", "")
			default:
				rec[field] = cast.ToString(v)
			}
		}
		records[i] = rec
	}
	return records
}
