package server

import (
	"io"
	"net/http"
	"strings"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	jsoniter "github.com/json-iterator/go"
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var standardRoutes = []echo.Route{
	{
		Method:  "GET",
		Path:    "/status",
		Handler: getStatus,
	},
	{
		Method:  "GET",
		Path:    "/.connections",
		Handler: getConnections,
	},
	{
		Method:  "GET",
		Path:    "/:connection/.databases",
		Handler: getConnectionDatabases,
	},
	{
		Method:  "GET",
		Path:    "/:connection/.schemas",
		Handler: getConnectionSchemas,
	},
	{
		Method:  "GET",
		Path:    "/:connection/.tables",
		Handler: getConnectionTables,
	},
	{
		Method:  "GET",
		Path:    "/:connection/.columns",
		Handler: getConnectionColumns,
	},
	{
		Method:  "POST",
		Path:    "/:connection/.sql",
		Handler: postConnectionSQL,
	},
	{
		Method:  "POST",
		Path:    "/:connection/.cancel",
		Handler: postConnectionCancel,
	},
	{
		Method:  "GET",
		Path:    "/:connection/:schema/.tables",
		Handler: getSchemaTables,
	},
	{
		Method:  "GET",
		Path:    "/:connection/:schema/.columns",
		Handler: getSchemaColumns,
	},
	{
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.columns",
		Handler: getTableColumns,
	},
	{
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.indexes",
		Handler: getTableIndexes,
	},
	{
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.keys",
		Handler: getTableKeys,
	},
	{
		Method:  "GET",
		Path:    "/:connection/:schema/:table",
		Handler: getTableSelect,
	},
	{
		Method:  "POST",
		Path:    "/:connection/:schema/:table",
		Handler: postTableInsertUpsert,
	},
	{
		Method:  "PATCH",
		Path:    "/:connection/:schema/:table",
		Handler: patchTableUpdate,
	},
}

func getStatus(c echo.Context) (err error) { return c.String(http.StatusOK, "OK") }

// Request is the typical request struct
type Request struct {
	Name        string            `json:"name" query:"name"`
	Connection  string            `json:"connection" query:"connection"`
	Database    string            `json:"database" query:"database"`
	Schema      string            `json:"schema" query:"schema"`
	Table       string            `json:"table" query:"table"`
	Query       string            `json:"query" query:"query"`
	Procedure   string            `json:"procedure" query:"procedure"`
	Data        any               `json:"data" query:"data"`
	Permissions state.Permissions `json:"-" query:"-"`
	ec          echo.Context      `json:"-" query:"-"`
}

func NewRequest(c echo.Context) Request {

	// TODO: token -> roles -> grants
	perms := state.Permissions{
		"*": state.PermissionReadWrite,
	} // TODO: generate permission map

	req := Request{ec: c, Permissions: perms}

	req.Connection = strings.ToUpper(c.PathParam("connection"))
	req.Schema = c.PathParam("schema")
	req.Table = c.PathParam("table")

	// parse table name
	conn, err := state.GetConnObject(req.Connection, "")
	if err != nil && req.Schema != "" {
		fullName := req.Schema + "." + req.Table
		table, _ := database.ParseTableName(fullName, conn.Type)
		req.Schema = table.Schema
		req.Table = table.Name
	}

	return req
}

type requestCheck string

const (
	reqCheckName       requestCheck = "name"
	reqCheckConnection requestCheck = "connection"
	reqCheckDatabase   requestCheck = "database"
	reqCheckSchema     requestCheck = "schema"
	reqCheckTable      requestCheck = "table"
	reqCheckQuery      requestCheck = "query"
	reqCheckProcedure  requestCheck = "procedure"
	reqCheckData       requestCheck = "data"
)

func (r *Request) CanRead(table database.Table) bool {
	if p, ok := r.Permissions["*"]; ok {
		if p.CanRead() {
			return true
		}
	}

	ts := state.SchemaAll(r.Connection, table.Schema)
	if p, ok := r.Permissions[ts.FullName()]; ok {
		if p.CanRead() {
			return true
		}
	}

	if p, ok := r.Permissions[table.FullName()]; ok {
		if p.CanRead() {
			return true
		}
	}

	return false
}

func (r *Request) CanWrite(table database.Table) bool {
	if p, ok := r.Permissions["*"]; ok {
		if p.CanWrite() {
			return true
		}
	}

	ts := state.SchemaAll(r.Connection, table.Schema)
	if p, ok := r.Permissions[ts.FullName()]; ok {
		if p.CanWrite() {
			return true
		}
	}

	if p, ok := r.Permissions[table.FullName()]; ok {
		if p.CanWrite() {
			return true
		}
	}

	return false
}

func (r *Request) Validate(checks ...requestCheck) (err error) {
	eG := g.ErrorGroup{}
	for _, check := range checks {
		switch check {
		case reqCheckName:
			if cast.ToString(r.Name) == "" {
				eG.Add(g.Error("missing request value for: name"))
			}
		case reqCheckConnection:
			if cast.ToString(r.Connection) == "" {
				eG.Add(g.Error("missing request value for: connection"))
			}
		case reqCheckDatabase:
			if cast.ToString(r.Database) == "" {
				eG.Add(g.Error("missing request value for: database"))
			}
		case reqCheckSchema:
			if cast.ToString(r.Schema) == "" {
				eG.Add(g.Error("missing request value for: schema"))
			}
		case reqCheckTable:
			if cast.ToString(r.Table) == "" {
				eG.Add(g.Error("missing request value for: table"))
			}
		case reqCheckQuery:
			if cast.ToString(r.Query) == "" {
				eG.Add(g.Error("missing request value for: query"))
			}
		case reqCheckProcedure:
			if cast.ToString(r.Procedure) == "" {
				eG.Add(g.Error("missing request value for: procedure"))
			}
		case reqCheckData:
			if cast.ToString(r.Data) == "" {
				eG.Add(g.Error("missing request value for: data"))
			}
		}
	}

	// TODO: validate grants
	return eG.Err()
}

type Response struct {
	Error string          `json:"error,omitempty"`
	ds    *iop.Datastream `json:"-"`
	data  iop.Dataset     `json:"-"`
	ec    echo.Context    `json:"-" query:"-"`
}

func NewResponse(c echo.Context) Response {
	return Response{ec: c}
}

func (r *Response) MakeStreaming(c echo.Context) (err error) {

	if r.ds == nil {
		return c.String(http.StatusOK, "")
	}
	////////////////////

	respW := c.Response().Writer
	var pushRow func(row []interface{})

	fields := r.ds.Columns.Names()
	contentType := strings.ToLower(c.Request().Header.Get(echo.HeaderContentType))

	switch contentType {
	case "text/plain", "text/csv":
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

		c.Response().Header().Set(echo.HeaderContentType, "text/csv")
	default:
		enc := json.NewEncoder(respW)
		pushRow = func(row []interface{}) {
			err := enc.Encode(row)
			if err != nil {
				r.ds.Context.Cancel()
				g.LogError(g.Error(err, "could not encode json record"))
			}
		}
		columnsI := lo.Map(r.ds.Columns, func(c iop.Column, i int) any {
			return g.M("name", c.Name, "type", c.Type, "dbType", c.DbType)
		})
		pushRow(columnsI) // first row is columns
		c.Response().Flush()

		c.Response().Header().Set(echo.HeaderContentType, "application/json")
	}

	// write headers
	c.Response().Header().Set("Transfer-Encoding", "chunked")
	c.Response().WriteHeader(http.StatusOK)
	c.Response().Flush()

	ctx := c.Request().Context()
	for row := range r.ds.Rows {

		select {
		case <-ctx.Done():
			r.ds.Context.Cancel()
			return
		default:
			pushRow(row)
			c.Response().Flush()
		}
	}

	return
}

func (r *Response) Make() (err error) {

	out := ""
	contentType := r.ec.Request().Header.Get(echo.HeaderContentType)
	switch strings.ToLower(contentType) {
	case "text/plain", "text/csv":
		r.ec.Response().Header().Set(echo.HeaderContentType, "text/csv")
		if r.ds != nil {
			reader := r.ds.NewCsvReader(0, 0)
			b, _ := io.ReadAll(reader)
			out = string(b)
		} else if len(r.data.Columns) > 0 {
			reader := r.data.Stream().NewCsvReader(0, 0)
			b, _ := io.ReadAll(reader)
			out = string(b)
		}
	default:
		r.ec.Response().Header().Set(echo.HeaderContentType, "application/json")
		if r.ds != nil {
			data, _ := r.ds.Collect(0)
			r.data = data
		}

		if len(r.data.Columns) > 0 {
			out = g.Marshal(r.data.Records())
		}
	}
	return r.ec.String(http.StatusOK, out)
}

// ReqFunction is the request function type
type ReqFunction func(c database.Connection, req Request) (iop.Dataset, error)

// ProcessRequest processes the request with the given function
func ProcessRequest(req Request, reqFunc ReqFunction) (data iop.Dataset, err error) {
	c, err := state.GetConnInstance(req.Connection, req.Database)
	if err != nil {
		err = g.Error(err, "could not get conn %s", req.Connection)
		return
	}

	return reqFunc(c, req)
}
