package remoteha

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	haconnect "github.com/litesql/go-ha/connect"
)

var (
	_ driver.Stmt              = (*stmt)(nil)
	_ driver.StmtExecContext   = (*stmt)(nil)
	_ driver.StmtQueryContext  = (*stmt)(nil)
	_ driver.NamedValueChecker = (*stmt)(nil)
	_ driver.Result            = (*result)(nil)
	_ driver.Rows              = (*rows)(nil)
	_ driver.Tx                = (*tx)(nil)
)

type stmt struct {
	query string
	conn  *conn
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), toNamedValues(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), toNamedValues(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.execContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.queryContext(ctx, s.query, args)
}

func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(nv)
}

func checkNamedValue(nv *driver.NamedValue) error {
	if nv.Value == nil {
		return nil
	}
	if _, ok := nv.Value.(uint64); ok {
		return nil
	}
	var err error
	nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
	return err
}

func toNamedValues(vals []driver.Value) []driver.NamedValue {
	r := make([]driver.NamedValue, len(vals))
	for i, val := range vals {
		r[i] = driver.NamedValue{Value: val, Ordinal: i + 1}
	}
	return r
}

func toParams(args []driver.NamedValue) ([]*sqlv1.NamedValue, error) {
	params := make([]*sqlv1.NamedValue, len(args))
	for i, arg := range args {
		val, err := haconnect.ToAnypb(arg.Value)
		if err != nil {
			return nil, err
		}
		params[i] = &sqlv1.NamedValue{
			Name:    arg.Name,
			Ordinal: int64(arg.Ordinal),
			Value:   val,
		}
	}
	return params, nil
}

type result struct {
	lastInsertId int64
	rowsAffected int64
}

func (r result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type rows struct {
	data  *sqlv1.Data
	index int
}

func (r *rows) Columns() []string {
	if r.data == nil {
		return []string{}
	}
	return r.data.GetColumns()
}

func (r *rows) Close() error {
	r.data = nil
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.data == nil || r.index >= len(r.data.Rows) {
		return io.EOF
	}
	row := r.data.Rows[r.index]
	values := row.GetValues()
	if len(values) > len(dest) {
		return fmt.Errorf("remote-ha: row has %d values, destination has %d", len(values), len(dest))
	}
	for i := range dest {
		if i < len(values) {
			dest[i] = normalizeDriverValue(haconnect.FromAnypb(values[i]))
		} else {
			dest[i] = nil
		}
	}
	r.index++
	return nil
}

func normalizeDriverValue(v any) driver.Value {
	switch val := v.(type) {
	case nil:
		return nil
	case int64, float64, bool, []byte, string, time.Time:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		if val <= math.MaxInt64 {
			return int64(val)
		}
		return strconv.FormatUint(val, 10)
	case float32:
		return float64(val)
	default:
		return fmt.Sprint(val)
	}
}

type tx struct {
	ctx  context.Context
	conn *conn
}

func (t *tx) Commit() error {
	_, err := t.conn.roundTrip(t.ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:  "COMMIT",
	})
	if err != nil {
		t.conn.markBadAndCancel()
		return err
	}
	return nil
}

func (t *tx) Rollback() error {
	// Use an independent context because database/sql invokes Rollback after t.ctx is canceled.
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()

	_, err := t.conn.roundTrip(ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:  "ROLLBACK",
	})
	if err != nil {
		t.conn.markBadAndCancel()
		return err
	}
	return nil
}
