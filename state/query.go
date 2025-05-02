package state

import (
	"database/sql/driver"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"gopkg.in/yaml.v3"
)

// Query represents a query
type Query struct {
	ID       string `json:"id" query:"id" gorm:"primaryKey"`
	Project  string `json:"project" query:"project" gorm:"index"`
	Conn     string `json:"conn" query:"conn" gorm:"index"`
	Database string `json:"database" query:"database" gorm:"index"`
	Text     string `json:"text" query:"text"`
	Limit    int    `json:"limit" query:"limit" gorm:"-"` // -1 is unlimited

	Start   int64       `json:"start" query:"start" gorm:"index:idx_start"`
	End     int64       `json:"end" query:"end"`
	Status  QueryStatus `json:"status" query:"status"`
	Err     string      `json:"err" query:"err"`
	Headers Headers     `json:"headers" query:"headers" gorm:"headers"`

	UpdatedDt   time.Time           `json:"-" gorm:"autoUpdateTime"`
	Connection  database.Connection `json:"-" gorm:"-"`
	Affected    int64               `json:"affected" gorm:"-"`
	Result      *sqlx.Rows          `json:"-" gorm:"-"`
	Stream      *iop.Datastream     `json:"-" gorm:"-"`
	Done        chan struct{}       `json:"-" gorm:"-"`
	Error       error               `json:"-" gorm:"-"`
	Context     *g.Context          `json:"-" gorm:"-"`
	lastTouch   time.Time           `json:"-" gorm:"-"`
	IsGenerated bool                `json:"-" gorm:"-"`
}

type QueryStatus string

const QueryStatusCompleted QueryStatus = "completed"
const QueryStatusFetched QueryStatus = "fetched"
const QueryStatusCancelled QueryStatus = "cancelled"
const QueryStatusErrored QueryStatus = "errored"
const QueryStatusSubmitted QueryStatus = "submitted"

type Headers []string

// Scan scan value into Jsonb, implements sql.Scanner interface
func (h *Headers) Scan(value interface{}) error {
	return g.JSONScanner(h, value)
}

// Value return json value, implement driver.Valuer interface
func (h Headers) Value() (driver.Value, error) {
	return g.JSONValuer(h, "[]")
}

type Row []interface{}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (r *Row) Scan(value interface{}) error {
	return g.JSONScanner(r, value)
}

// Value return json value, implement driver.Valuer interface
func (r Row) Value() (driver.Value, error) {
	return g.JSONValuer(r, "[]")
}

type Rows [][]interface{}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (r *Rows) Scan(value interface{}) error {
	return g.JSONScanner(r, value)
}

// Value return json value, implement driver.Valuer interface
func (r Rows) Value() (driver.Value, error) {
	return g.JSONValuer(r, "[]")
}

func SubmitOrGetQuery(q *Query, cont bool) (query *Query, err error) {
	if cont {

		proj := LoadProject(q.Project)
		if proj == nil {
			return query, g.Error("unable to load project %s", q.Project)
		}

		// pick up where left off
		mux.Lock()
		var ok bool
		query, ok = proj.Queries[q.ID]
		if ok {
			query.lastTouch = time.Now()
		}
		mux.Unlock()

		if !ok {
			err = g.Error("could not find query %s to continue", q.ID)
			return
		}
	} else {
		err = q.prepare()
		if err != nil {
			err = g.Error(err, "could not prepare query")
			return
		}
		// submit
		go q.Submit()
		query = q
	}

	return
}

func (q *Query) Cancel() (err error) {
	id := q.ID

	proj := LoadProject(q.Project)
	if proj == nil {
		return g.Error("unable to load project %s", q.Project)
	}

	mux.Lock()
	q, ok := proj.Queries[id]
	mux.Unlock()
	if !ok {
		err = g.Error("could not find query %s", id)
		return
	}

	err = q.Close(true)
	if err != nil {
		err = g.Error(err, "could not close query %s", id)
		return
	}

	q.Status = QueryStatusCancelled

	mux.Lock()
	delete(proj.Queries, q.ID)
	mux.Unlock()

	return
}

func (q *Query) Submit() (err error) {
	defer func() { q.Done <- struct{}{} }()

	setError := func(err error) {
		q.Status = QueryStatusErrored
		q.Error = err
		q.Err = g.ErrMsg(err)
		q.End = time.Now().Unix()
	}

	q.Status = QueryStatusSubmitted
	q.Context = g.NewContext(q.Connection.Context().Ctx)

	sqls := database.ParseSQLMultiStatements(q.Text)
	if len(sqls) == 1 && q.isSelecting() {
		g.Debug("--------------------------------------------------------------------- submitting %s (selecting)", q.ID)
		q.Stream, err = q.Connection.StreamRowsContext(q.Context.Ctx, q.Text, g.M("limit", q.Limit))
		if err != nil {
			setError(err)
			err = g.Error(err, "could not execute query")
			return
		}

		q.Status = QueryStatusCompleted
	} else {
		g.Debug("--------------------------------------------------------------------- submitting %s (executing)", q.ID)
		_, err = q.Connection.NewTransaction(q.Context.Ctx)
		if err != nil {
			setError(err)
			err = g.Error(err, "could not start transaction")
			return err
		}

		defer q.Connection.Rollback()
		res, err := q.Connection.ExecMultiContext(q.Context.Ctx, q.Text)
		if err != nil {
			setError(err)
			err = g.Error(err, "could not execute queries")
			return err
		}
		err = q.Connection.Commit()
		if err != nil {
			setError(err)
			err = g.Error(err, "could not commit")
			return err
		}

		q.Status = QueryStatusCompleted
		q.Affected, _ = res.RowsAffected()
	}

	return
}

// processCustomReq looks at the text for yaml parsing
func (q *Query) prepare() (err error) {

	// get connection
	proj := LoadProject(q.Project)
	if proj == nil {
		return g.Error("unable to load project %s", q.Project)
	}

	q.Connection, err = proj.GetConnInstance(q.Conn, q.Database)
	if err != nil {
		err = g.Error(err, "could not get conn %s", q.Conn)
		return
	}

	err = q.processCustomReq()
	if err != nil {
		err = g.Error(err, "could not get templatized sql")
		return
	}

	mux.Lock()
	proj.Queries[q.ID] = q
	mux.Unlock()

	q.Text = strings.TrimSuffix(q.Text, ";")

	return nil
}

func (q *Query) processCustomReq() (err error) {

	// see if analysis req
	if strings.HasPrefix(q.Text, "/*--") && strings.HasSuffix(q.Text, "--*/") {
		// is data request in yaml or json
		// /*--{"analysis":"field_count", "data": {...}} --*/
		// /*--{"metadata":"ddl_table", "data": {...}} --*/
		type analysisReq struct {
			Analysis string                 `json:"analysis" yaml:"analysis"`
			Metadata string                 `json:"metadata" yaml:"metadata"`
			Data     map[string]interface{} `json:"data" yaml:"data"`
		}

		req := analysisReq{}
		body := strings.TrimSuffix(strings.TrimPrefix(q.Text, "/*--"), "--*/")
		err = yaml.Unmarshal([]byte(body), &req)
		if err != nil {
			err = g.Error(err, "could not parse yaml/json request")
			return
		}

		sql := ""
		switch {
		case req.Analysis != "":
			sql, err = q.Connection.GetAnalysis(req.Analysis, req.Data)
		case req.Metadata != "":
			template, ok := q.Connection.Template().Metadata[req.Metadata]
			if !ok {
				err = g.Error("metadata key '%s' not found", req.Metadata)
			}
			sql = g.Rm(template, req.Data)
		}

		if err != nil {
			err = g.Error(err, "could not execute query")
			return
		}

		q.Text = q.Text + "\n\n" + sql
		q.IsGenerated = true
	}
	return
}

// isSelecting detects whether a query is a SELECT query
func (q *Query) isSelecting() bool {
	// Parse the SQL statements
	sqls := database.ParseSQLMultiStatements(q.Text)

	// Check each statement
	for _, sql := range sqls {
		normalizedSQL := strings.ToLower(strings.TrimSpace(sql))
		if strings.HasPrefix(normalizedSQL, "select") ||
			strings.HasPrefix(normalizedSQL, "with") ||
			(strings.Contains(normalizedSQL, "select") && strings.Contains(normalizedSQL, "from")) {
			return true
		}
	}

	return false
}

// Close closes and cancels the query
func (q *Query) Close(cancel bool) (err error) {
	if cancel {
		q.Context.Cancel()
	}
	if q.Result != nil {
		err = q.Result.Close()
		if err != nil {
			return g.Error(err, "could not close results")
		}
	}
	return
}

func (q *Query) ProcessResult() (err error) {

	proj := LoadProject(q.Project)
	if proj == nil {
		return g.Error("unable to load project %s", q.Project)
	}

	// delete query from map
	mux.Lock()
	delete(proj.Queries, q.ID)

	mux.Unlock()

	if q.Error != nil {
		return q.Error
	}

	if q.Affected == -1 && q.Stream != nil {
		q.Headers = q.Stream.Columns.Names()
		g.Debug("buffered %d rows", len(q.Stream.Buffer))
	}

	q.Close(false)

	q.End = time.Now().Unix()

	return
}
