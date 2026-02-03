package remoteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"connectrpc.com/connect"
	"github.com/litesql/go-ha/api/sql/v1/sqlv1connect"
	haconnect "github.com/litesql/go-ha/connect"
)

func init() {
	sql.Register("remote-ha", &Driver{})
}

var drv driver.Driver = &Driver{}

type Driver struct {
}

const defaultTimeout = 15 * time.Second

func (d *Driver) Open(name string) (driver.Conn, error) {
	return doConnect(context.Background(), name, defaultTimeout)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &Connector{
		Name:    name,
		Timeout: defaultTimeout,
	}, nil

}

type Connector struct {
	Name    string
	Timeout time.Duration
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return doConnect(ctx, c.Name, c.Timeout)
}

func (c *Connector) Driver() driver.Driver {
	return drv
}

func doConnect(ctx context.Context, name string, timeout time.Duration) (driver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}

	opts := make([]connect.ClientOption, 0)
	if u.User != nil {
		if pass, ok := u.User.Password(); ok {
			opts = append(opts, connect.WithInterceptors(haconnect.NewAuthInterceptor(pass)))
		}
		u.User = nil
	}

	p := new(http.Protocols)
	p.SetUnencryptedHTTP2(true)
	if timeout == 0 {
		timeout = defaultTimeout
	}
	client := sqlv1connect.NewDatabaseServiceClient(&http.Client{
		Transport: &http.Transport{
			Protocols: p,
		},
		Timeout: timeout,
	}, u.String(), opts...)
	ctx, cancel := context.WithCancel(ctx)
	return newConn(client.Query(ctx), cancel), nil
}
