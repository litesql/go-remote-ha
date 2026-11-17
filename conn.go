package remoteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"google.golang.org/grpc"
)

const rollbackTimeout = 10 * time.Second

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
)

type conn struct {
	mu            sync.Mutex
	stream        grpc.BidiStreamingClient[sqlv1.QueryRequest, sqlv1.QueryResponse]
	streamCtx     context.Context
	cancel        context.CancelFunc
	ownCC         *grpc.ClientConn
	replicationID string

	lastActive atomic.Int64
	badConn    atomic.Bool
	closed     atomic.Bool
	doneCh     chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

func newConn(
	replicationID string,
	stream grpc.BidiStreamingClient[sqlv1.QueryRequest, sqlv1.QueryResponse],
	streamCtx context.Context,
	cancel context.CancelFunc,
	ownCC *grpc.ClientConn,
	keepaliveInterval time.Duration,
) *conn {
	c := &conn{
		stream:        stream,
		streamCtx:     streamCtx,
		cancel:        cancel,
		ownCC:         ownCC,
		replicationID: replicationID,
		doneCh:        make(chan struct{}),
	}
	c.lastActive.Store(time.Now().UnixNano())

	if keepaliveInterval > 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.keepaliveLoop(keepaliveInterval)
		}()
	}

	return c
}

func (c *conn) keepaliveLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.doneCh:
			return
		case <-ticker.C:
			if c.closed.Load() || c.badConn.Load() {
				return
			}
			last := time.Unix(0, c.lastActive.Load())
			if time.Since(last) < interval {
				continue
			}
			if !c.sendKeepalivePing(interval) {
				return
			}
		}
	}
}

func (c *conn) sendKeepalivePing(timeout time.Duration) bool {
	if !c.mu.TryLock() {
		// A query is actively using the stream; skip keepalive ping.
		return true
	}
	defer c.mu.Unlock()

	if c.closed.Load() || c.badConn.Load() {
		return false
	}

	pingCtx, cancel := context.WithTimeout(c.streamCtx, timeout)
	defer cancel()

	stopCancel := context.AfterFunc(pingCtx, func() {
		c.markBadAndCancel()
	})
	err := c.stream.Send(&sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_PING,
		ReplicationId: c.replicationID,
	})
	var resp *sqlv1.QueryResponse
	if err == nil {
		resp, err = c.stream.Recv()
	}
	canceled := !stopCancel()
	if err != nil || canceled || resp.GetError() != "" {
		c.markBadAndCancel()
		return false
	}

	c.lastActive.Store(time.Now().UnixNano())
	return true
}

func (c *conn) markBadAndCancel() {
	c.badConn.Store(true)
	c.cancel()
}

func (c *conn) roundTrip(ctx context.Context, req *sqlv1.QueryRequest) (*sqlv1.QueryResponse, error) {
	if c.closed.Load() || c.badConn.Load() {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() || c.badConn.Load() {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	req.ReplicationId = c.replicationID

	// If ctx expires while Send/Recv is in flight, the stream becomes desynchronized,
	// so mark it bad and cancel streamCtx to unblock Send/Recv immediately.
	stopCancel := context.AfterFunc(ctx, func() {
		c.markBadAndCancel()
	})
	err := c.stream.Send(req)
	var resp *sqlv1.QueryResponse
	if err == nil {
		resp, err = c.stream.Recv()
	}
	canceledByCtx := !stopCancel()

	if canceledByCtx {
		c.markBadAndCancel()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, driver.ErrBadConn
	}
	if err != nil {
		c.markBadAndCancel()
		return nil, fmt.Errorf("%w: %v", driver.ErrBadConn, err)
	}

	c.lastActive.Store(time.Now().UnixNano())

	if resp.GetError() != "" {
		return nil, errors.New(resp.GetError())
	}
	return resp, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.closed.Load() || c.badConn.Load() {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &stmt{query: query, conn: c}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.execContext(ctx, query, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.queryContext(ctx, query, args)
}

func (c *conn) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	params, err := toParams(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.roundTrip(ctx, &sqlv1.QueryRequest{
		Type:   sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:    query,
		Params: params,
	})
	if err != nil {
		return nil, err
	}
	return result{
		lastInsertId: resp.GetLastInsertId(),
		rowsAffected: resp.GetRowsAffected(),
	}, nil
}

func (c *conn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	params, err := toParams(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.roundTrip(ctx, &sqlv1.QueryRequest{
		Type:   sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY,
		Sql:    query,
		Params: params,
	})
	if err != nil {
		return nil, err
	}
	return &rows{data: resp.GetResultSet()}, nil
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(nv)
}

func (c *conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.badConn.Store(true)
		close(c.doneCh)

		if c.mu.TryLock() {
			err = c.stream.CloseSend()
			c.cancel()
			c.mu.Unlock()
		} else {
			c.cancel()
			c.mu.Lock()
			c.mu.Unlock()
		}

		c.wg.Wait()

		if c.ownCC != nil {
			if closeErr := c.ownCC.Close(); err == nil {
				err = closeErr
			}
		}
	})
	return err
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if level := sql.IsolationLevel(opts.Isolation); level != sql.LevelDefault && level != sql.LevelSerializable {
		return nil, fmt.Errorf("remote-ha: unsupported isolation level: %s", level)
	}
	if opts.ReadOnly {
		return nil, errors.New("remote-ha: read-only transactions are not supported")
	}

	_, err := c.roundTrip(ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:  "BEGIN",
	})
	if err != nil {
		return nil, err
	}
	return &tx{
		ctx:  ctx,
		conn: c,
	}, nil
}

func (c *conn) IsValid() bool {
	return !c.closed.Load() && !c.badConn.Load()
}

func (c *conn) ResetSession(ctx context.Context) error {
	if !c.IsValid() {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) Ping(ctx context.Context) error {
	_, err := c.roundTrip(ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY,
		Sql:  "SELECT 1",
	})
	return err
}
