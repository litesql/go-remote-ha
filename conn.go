package remoteha

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"google.golang.org/grpc"
)

type streamResponse struct {
	*sqlv1.QueryResponse
	BadConn bool
}

type conn struct {
	stream     grpc.BidiStreamingClient[sqlv1.QueryRequest, sqlv1.QueryResponse]
	cancel     func()
	requestCh  chan *sqlv1.QueryRequest
	responseCh chan *streamResponse
	badConn    bool
}

func newConn(stream grpc.BidiStreamingClient[sqlv1.QueryRequest, sqlv1.QueryResponse], cancel func()) *conn {
	requestCh := make(chan *sqlv1.QueryRequest)
	responseCh := make(chan *streamResponse)
	go func() {
		for {
			resp, err := stream.Recv()
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

	c := &conn{
		stream:     stream,
		cancel:     cancel,
		requestCh:  requestCh,
		responseCh: responseCh,
	}

	go func() {
		for {
			select {
			case req := <-requestCh:
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
			case <-time.After(25 * time.Second):
				ex := c.newExecutor()
				resp := ex.exec(context.Background(), &sqlv1.QueryRequest{
					Type: sqlv1.QueryType_QUERY_TYPE_PING,
				})
				if resp.BadConn {
					c.badConn = true
					return
				}
			}
		}
	}()

	return c
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{query: query, ex: c.newExecutor()}, nil
}

func (c *conn) newExecutor() *executor {
	return &executor{
		requestCh:  c.requestCh,
		responseCh: c.responseCh,
		badConn:    c.badConn,
	}
}

func (c *conn) Close() error {
	c.cancel()
	close(c.requestCh)
	return c.stream.CloseSend()
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
	badConn    bool
}

func (e *executor) exec(ctx context.Context, req *sqlv1.QueryRequest) *streamResponse {
	if e.badConn {
		return &streamResponse{
			BadConn: true,
			QueryResponse: &sqlv1.QueryResponse{
				Error: "bad connection",
			},
		}
	}
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
