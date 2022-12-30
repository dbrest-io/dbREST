package state

import (
	"context"
	"database/sql/driver"
	"strings"
	"time"

	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
)

// NewQuery creates a Query object
func NewQuery(ctx context.Context) *Query {
	q := new(Query)
	q.Affected = -1
	q.Context = g.NewContext(ctx)
	q.Done = make(chan struct{})
	return q
}

// Query represents a query
type Query struct {
	ID       string `json:"id" query:"id" gorm:"primaryKey"`
	Conn     string `json:"conn" query:"conn" gorm:"index"`
	Database string `json:"database" query:"database" gorm:"index"`
	Text     string `json:"text" query:"text"`
	Limit    int    `json:"limit" query:"limit" gorm:"-"` // -1 is unlimited

	Start   int64       `json:"start" query:"start" gorm:"index:idx_start"`
	End     int64       `json:"end" query:"end"`
	Status  QueryStatus `json:"status" query:"status"`
	Error   string      `json:"error" query:"error"`
	Headers Headers     `json:"headers" query:"headers" gorm:"headers"`

	UpdatedDt time.Time       `json:"-" gorm:"autoUpdateTime"`
	Affected  int64           `json:"affected" gorm:"-"`
	Result    *sqlx.Rows      `json:"-" gorm:"-"`
	Stream    *iop.Datastream `json:"-" gorm:"-"`
	Done      chan struct{}   `json:"-" gorm:"-"`
	Err       error           `json:"-" gorm:"-"`
	Context   g.Context       `json:"-" gorm:"-"`
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
		// pick up where left off
		mux.Lock()
		var ok bool
		qId := query.ID
		query, ok = Queries[query.ID]
		mux.Unlock()
		if !ok {
			err = g.Error("could not find query %s to continue", qId)
			return
		}
	} else {
		// submit
		go q.submit()
		query = q
	}

	return
}

func (q *Query) Cancel() (err error) {
	id := q.ID
	mux.Lock()
	q, ok := Queries[id]
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
	delete(Queries, q.ID)
	mux.Unlock()

	return
}

func (q *Query) submit() (err error) {
	defer func() { q.Done <- struct{}{} }()

	setError := func(err error) {
		q.Status = QueryStatusErrored
		q.Err = err
		q.Error = g.ErrMsg(err)
		q.End = time.Now().Unix()
	}

	// get connection
	conn, err := GetConnInstance(q.Conn, q.Database)
	if err != nil {
		err = g.Error(err, "could not get conn %s", q.Conn)
		setError(err)
		return
	}

	mux.Lock()
	Queries[q.ID] = q
	mux.Unlock()

	// expire the query after 10 minutes
	timer := time.NewTimer(time.Duration(10*60) * time.Second)
	go func() {
		<-timer.C
		q.Close(false)
		mux.Lock()
		delete(Queries, q.ID)
		mux.Unlock()
	}()

	q.Text = strings.TrimSuffix(q.Text, ";")

	q.Status = QueryStatusSubmitted
	q.Context = g.NewContext(conn.Context().Ctx)

	g.Debug("--------------------------------------------------------------------- submitting %s", q.ID)

	sqls := database.ParseSQLMultiStatements(q.Text)
	if len(sqls) == 1 {
		q.Stream, err = conn.StreamRowsContext(q.Context.Ctx, q.Text, g.M("limit", q.Limit))
		if err != nil {
			setError(err)
			err = g.Error(err, "could not execute query")
			return
		}

		q.Status = QueryStatusCompleted
	} else {
		_, err = conn.NewTransaction(q.Context.Ctx)
		if err != nil {
			setError(err)
			err = g.Error(err, "could not start transaction")
			return err
		}

		defer conn.Rollback()
		res, err := conn.ExecMultiContext(q.Context.Ctx, q.Text)
		if err != nil {
			setError(err)
			err = g.Error(err, "could not execute queries")
			return err
		}
		err = conn.Commit()
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
	// delete query from map
	mux.Lock()
	delete(Queries, q.ID)
	mux.Unlock()

	if q.Err != nil {
		return q.Err
	}

	if q.Affected == -1 && q.Stream != nil {
		q.Headers = q.Stream.Columns.Names()
		g.Debug("buffered %d rows", len(q.Stream.Buffer))
	}

	q.Close(false)

	q.End = time.Now().Unix()

	return
}
