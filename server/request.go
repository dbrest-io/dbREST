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
	"github.com/labstack/echo/v5"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

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
	Roles       state.RoleMap     `json:"-" query:"-"`
	Permissions state.Permissions `json:"-" query:"-"`
	echoCtx     echo.Context      `json:"-" query:"-"`
}

func NewRequest(c echo.Context) Request {

	req := Request{
		ID:          c.PathParam("id"),
		Connection:  strings.ToLower(c.PathParam("connection")),
		Schema:      c.PathParam("schema"),
		Table:       c.PathParam("table"),
		Database:    c.QueryParam("database"),
		echoCtx:     c,
		Header:      c.Request().Header,
		Roles:       state.RoleMap{},
		Permissions: state.Permissions{
			// "*": state.PermissionRead, // read access
			// "*": state.PermissionReadWrite, // read/write access
		},
	}

	req.ID = lo.Ternary(req.ID == "", c.QueryParam("id"), req.ID)
	req.Schema = lo.Ternary(req.Schema == "", c.QueryParam("schema"), req.Schema)

	// load tokens, do not force, cached & throttled
	state.LoadTokens(false)

	// token -> roles -> grants
	if authToken := c.Request().Header.Get("Authorization"); authToken != "" {
		token, ok := state.ResolveToken(authToken)
		if ok && !token.Disabled {
			req.Roles = state.GetRoleMap(token.Roles)
			req.Permissions = req.Roles.GetPermissions(req.Connection)
		}
	}

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
			} else if !(r.CanRead(r.dbTable) || r.CanWrite(r.dbTable)) {
				eG.Add(g.Error("forbidden access for: table"))
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

	return eG.Err()
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
