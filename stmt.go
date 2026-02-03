package remoteha

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	haconnect "github.com/litesql/go-ha/connect"
)

type stmt struct {
	query string
	ex    *executor
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
	params, err := toParams(args)
	if err != nil {
		return nil, err
	}

	resp := s.ex.exec(ctx, &sqlv1.QueryRequest{
		Type:   sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:    s.query,
		Params: params,
	})
	if resp.Error != "" {
		if resp.BadConn {
			return nil, fmt.Errorf("%w: %s", driver.ErrBadConn, resp.Error)
		}
		return nil, errors.New(resp.Error)
	}
	return result{
		rowsAffected: resp.RowsAffected,
	}, nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	params, err := toParams(args)
	if err != nil {
		return nil, err
	}

	resp := s.ex.exec(ctx, &sqlv1.QueryRequest{
		Type:   sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY,
		Sql:    s.query,
		Params: params,
	})
	if resp.Error != "" {
		if resp.BadConn {
			return nil, fmt.Errorf("%w: %s", driver.ErrBadConn, resp.Error)
		}
		return nil, errors.New(resp.Error)
	}

	return &rows{data: resp.ResultSet}, nil
}

func toNamedValues(vals []driver.Value) (r []driver.NamedValue) {
	r = make([]driver.NamedValue, len(vals))
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
	if r.data == nil || r.data.Rows == nil || r.index >= len(r.data.Rows) {
		return io.EOF
	}
	row := r.data.Rows[r.index]
	for i, val := range row.GetValues() {
		dest[i] = haconnect.FromAnypb(val)
	}
	r.index++
	return nil
}

type tx struct {
	ctx context.Context
	ex  *executor
}

func (tx *tx) Commit() error {
	resp := tx.ex.exec(tx.ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:  "COMMIT",
	})
	if resp.Error != "" {
		if resp.BadConn {
			return fmt.Errorf("%w: %s", driver.ErrBadConn, resp.Error)
		}
		return errors.New(resp.Error)
	}
	return nil
}

func (tx *tx) Rollback() error {
	resp := tx.ex.exec(tx.ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:  "ROLLBACK",
	})
	if resp.Error != "" {
		if resp.BadConn {
			return fmt.Errorf("%w: %s", driver.ErrBadConn, resp.Error)
		}
		return errors.New(resp.Error)
	}
	return nil
}
