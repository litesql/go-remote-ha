package remoteha

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	sql.Register("remote-ha", &Driver{})
}

var drv driver.Driver = &Driver{}

type Driver struct {
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	return doConnect(context.Background(), name)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &Connector{
		Name: name,
	}, nil

}

type Connector struct {
	Name string
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return doConnect(ctx, c.Name)
}

func (c *Connector) Driver() driver.Driver {
	return drv
}

func doConnect(ctx context.Context, name string) (driver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	opts := make([]grpc.DialOption, 0)
	if strings.HasPrefix(name, "http://") {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	if u.User != nil {
		if pass, ok := u.User.Password(); ok {
			opts = append(opts, grpc.WithPerRPCCredentials(grpcCredentials{token: pass}))
		}
		u.User = nil
	}

	cc, err := grpc.NewClient(u.Host, opts...)
	if err != nil {
		return nil, fmt.Errorf("create grpc client: %w", err)
	}

	client := sqlv1.NewDatabaseServiceClient(cc)

	ctx, cancel := context.WithCancel(ctx)
	queryStream, err := client.Query(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create query stream: %w", err)
	}
	return newConn(queryStream, cancel), nil
}

type grpcCredentials struct {
	token string
}

func (c grpcCredentials) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.token,
	}, nil
}

func (c grpcCredentials) RequireTransportSecurity() bool {
	return false
}
