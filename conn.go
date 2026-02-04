package remoteha

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"connectrpc.com/connect"
	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
)

type streamResponse struct {
	*sqlv1.QueryResponse
	BadConn bool
}

type conn struct {
	stream     *connect.BidiStreamForClient[sqlv1.QueryRequest, sqlv1.QueryResponse]
	cancel     func()
	requestCh  chan *sqlv1.QueryRequest
	responseCh chan *streamResponse
}

func newConn(stream *connect.BidiStreamForClient[sqlv1.QueryRequest, sqlv1.QueryResponse], cancel func()) *conn {
	requestCh := make(chan *sqlv1.QueryRequest)
	responseCh := make(chan *streamResponse)
	go func() {
		for {
			resp, err := stream.Receive()
			if err != nil {
				if err == io.EOF {
					return
				}
				responseCh <- &streamResponse{
					BadConn: true,
					QueryResponse: &sqlv1.QueryResponse{
						Error: err.Error(),
					},
				}
				return
			}
			responseCh <- &streamResponse{
				QueryResponse: resp,
			}
		}
	}()

	go func() {
		for req := range requestCh {
			err := stream.Send(req)
			if err != nil {
				responseCh <- &streamResponse{
					BadConn: true,
					QueryResponse: &sqlv1.QueryResponse{
						Error: err.Error(),
					},
				}
				return
			}
		}
	}()

	return &conn{
		stream:     stream,
		cancel:     cancel,
		requestCh:  requestCh,
		responseCh: responseCh,
	}
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{query: query, ex: c.newExecutor()}, nil
}

func (c *conn) newExecutor() *executor {
	return &executor{
		requestCh:  c.requestCh,
		responseCh: c.responseCh,
	}
}

func (c *conn) Close() error {
	streamConn, err := c.stream.Conn()
	if err == nil {
		err = errors.Join(err, streamConn.CloseRequest())
		err = errors.Join(err, streamConn.CloseResponse())
	}
	c.cancel()
	close(c.requestCh)
	return err
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	ex := c.newExecutor()
	resp := ex.exec(ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY,
		Sql:  "BEGIN",
	})
	if resp.Error != "" {
		if resp.BadConn {
			return nil, fmt.Errorf("%w: %s", driver.ErrBadConn, resp.Error)
		}
		return nil, errors.New(resp.Error)
	}
	return &tx{
		ctx: ctx,
		ex:  ex,
	}, nil
}

func (c *conn) IsValid() bool {
	resp := c.newExecutor().exec(context.Background(), &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY,
		Sql:  "SELECT 1",
	})
	return resp.Error == ""
}

func (c *conn) Ping(ctx context.Context) error {
	resp := c.newExecutor().exec(ctx, &sqlv1.QueryRequest{
		Type: sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY,
		Sql:  "SELECT 1",
	})
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	return nil
}

type executor struct {
	requestCh  chan *sqlv1.QueryRequest
	responseCh chan *streamResponse
}

func (e *executor) exec(ctx context.Context, req *sqlv1.QueryRequest) *streamResponse {
	go func() {
		select {
		case e.requestCh <- req:
		case <-ctx.Done():
			return
		}
	}()

	select {
	case resp := <-e.responseCh:
		return resp
	case <-ctx.Done():
		return &streamResponse{
			QueryResponse: &sqlv1.QueryResponse{
				Error: ctx.Err().Error(),
			},
		}
	}
}
