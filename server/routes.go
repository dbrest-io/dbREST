package server

import (
	"io"
	"net/http"
	"strings"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/filesys"
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
		Name:    "getStatus",
		Method:  "GET",
		Path:    "/.status",
		Handler: getStatus,
	},
	{
		Name:    "getConnections",
		Method:  "GET",
		Path:    "/.connections",
		Handler: getConnections,
	},
	{
		Name:    "getConnectionDatabases",
		Method:  "GET",
		Path:    "/:connection/.databases",
		Handler: getConnectionDatabases,
	},
	{
		Name:    "getConnectionSchemas",
		Method:  "GET",
		Path:    "/:connection/.schemas",
		Handler: getConnectionSchemas,
	},
	{
		Name:    "getConnectionTables",
		Method:  "GET",
		Path:    "/:connection/.tables",
		Handler: getConnectionTables,
	},
	{
		Name:    "getConnectionColumns",
		Method:  "GET",
		Path:    "/:connection/.columns",
		Handler: getConnectionColumns,
	},
	{
		Name:    "submitSQL",
		Method:  "POST",
		Path:    "/:connection/.sql",
		Handler: postConnectionSQL,
	},
	{
		Name:    "submitSQL_ID",
		Method:  "POST",
		Path:    "/:connection/.sql/:id",
		Handler: postConnectionSQL,
	},
	{
		Name:    "cancelSQL",
		Method:  "POST",
		Path:    "/:connection/.cancel/:id",
		Handler: postConnectionCancel,
	},
	{
		Name:    "getSchemaTables",
		Method:  "GET",
		Path:    "/:connection/:schema/.tables",
		Handler: getSchemaTables,
	},
	{
		Name:    "getSchemaColumns",
		Method:  "GET",
		Path:    "/:connection/:schema/.columns",
		Handler: getSchemaColumns,
	},
	{
		Name:    "getTableColumns",
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.columns",
		Handler: getTableColumns,
	},
	{
		Name:    "getTableIndexes",
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.indexes",
		Handler: getTableIndexes,
	},
	{
		Name:    "getTableKeys",
		Method:  "GET",
		Path:    "/:connection/:schema/:table/.keys",
		Handler: getTableKeys,
	},
	{
		Name:    "tableInsert",
		Method:  "POST",
		Path:    "/:connection/:schema/:table",
		Handler: postTableInsert,
	},
	{
		Name:    "tableUpsert",
		Method:  "PUT",
		Path:    "/:connection/:schema/:table",
		Handler: postTableUpsert,
	},
	{
		Name:    "tableUpdate",
		Method:  "PATCH",
		Path:    "/:connection/:schema/:table",
		Handler: patchTableUpdate,
	},
	{
		Name:    "getTableSelect",
		Method:  "GET",
		Path:    "/:connection/:schema/:table",
		Handler: getTableSelect,
	},
}

func getStatus(c echo.Context) (err error) { return c.String(http.StatusOK, "OK") }

// Request is the typical request struct
type Request struct {
	ID         string `json:"id" query:"id"` // used for query ID
	Name       string `json:"name" query:"name"`
	Connection string `json:"connection" query:"connection"`
	Database   string `json:"database" query:"database"`
	Schema     string `json:"schema" query:"schema"`
	Table      string `json:"table" query:"table"`
	Query      string `json:"query" query:"query"`
	Procedure  string `json:"procedure" query:"procedure"`
	Data       any    `json:"data" query:"data"`

	Header      http.Header       `json:"-" query:"-"`
	dbTable     database.Table    `json:"-" query:"-"`
	Permissions state.Permissions `json:"-" query:"-"`
	echoCtx     echo.Context      `json:"-" query:"-"`
}

func NewRequest(c echo.Context) Request {

	// TODO: token -> roles -> grants
	perms := state.Permissions{
		"*": state.PermissionReadWrite,
	} // TODO: generate permission map

	req := Request{
		ID:          c.PathParam("id"),
		Connection:  strings.ToUpper(c.PathParam("connection")),
		Schema:      c.PathParam("schema"),
		Table:       c.PathParam("table"),
		Database:    c.QueryParam("database"),
		echoCtx:     c,
		Header:      c.Request().Header,
		Permissions: perms,
	}

	req.ID = lo.Ternary(req.ID == "", c.QueryParam("id"), req.ID)
	req.Schema = lo.Ternary(req.Schema == "", c.QueryParam("schema"), req.Schema)

	// parse table name
	conn, err := state.GetConnObject(req.Connection, "")
	if err == nil && req.Schema != "" {
		if req.Table != "" {
			fullName := req.Schema + "." + req.Table
			req.dbTable, _ = database.ParseTableName(fullName, conn.Type)
			req.Schema = req.dbTable.Schema
			req.Table = req.dbTable.Name
		} else {
			fullName := req.Schema + ".*"
			t, _ := database.ParseTableName(fullName, conn.Type)
			req.Schema = t.Schema
		}
	}

	return req
}

type requestCheck string

const (
	reqCheckID         requestCheck = "id"
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

func (req *Request) GetDatastream() (ds *iop.Datastream, err error) {
	ctx := req.echoCtx.Request().Context()

	// whether to flatten json, default is true
	flatten := req.echoCtx.QueryParam("flatten")

	cfg := map[string]string{
		"flatten":         lo.Ternary(flatten == "", "true", flatten),
		"delimiter":       req.echoCtx.QueryParam("delimiter"),
		"header":          req.echoCtx.QueryParam("header"),
		"datetime_format": req.echoCtx.QueryParam("datetime_format"),
	}
	ds = iop.NewDatastreamContext(ctx, nil)
	ds.SafeInference = true
	ds.SetConfig(cfg)

	contentType := strings.ToLower(req.Header.Get("Content-Type"))
	reader := req.echoCtx.Request().Body

	switch contentType {
	case "multipart/form-data":
		reader, err = req.GetFileUpload()
		if err != nil {
			err = g.Error(err, "could not get file reader")
			return
		}

		ft, reader, err := filesys.PeekFileType(reader)
		if err != nil {
			err = g.Error(err, "could not peek file reader")
			return ds, err
		}

		switch ft {
		case filesys.FileTypeCsv:
			err = ds.ConsumeCsvReader(reader)
		case filesys.FileTypeXml:
			err = ds.ConsumeXmlReader(reader)
		case filesys.FileTypeJson:
			err = ds.ConsumeJsonReader(reader)
		}
		if err != nil {
			err = g.Error(err, "could not consume reader")
			return ds, err
		}
	case "text/plain", "text/csv":
		err = ds.ConsumeCsvReader(reader)
	case "application/xml":
		err = ds.ConsumeXmlReader(reader)
	default:
		err = ds.ConsumeJsonReader(reader)
	}
	if err != nil {
		err = g.Error(err, "could not consume reader")
		return
	}

	err = ds.WaitReady()
	if err != nil {
		err = g.Error(err, "error waiting for datastream")
		return
	}

	return
}

func (r *Request) GetFileUpload() (src io.ReadCloser, err error) {
	file, err := r.echoCtx.FormFile("file")
	if err != nil {
		err = g.Error(err, "could not open form file")
		return
	}

	src, err = file.Open()
	if err != nil {
		err = g.Error(err, "could not open file")
		return
	}

	return
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
		case reqCheckID:
			if r.ID == "" {
				eG.Add(g.Error("missing request value for: id"))
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
