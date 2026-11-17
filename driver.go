package remoteha

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultKeepaliveInterval = 25 * time.Second

func init() {
	sql.Register("remote-ha", drv)
}

var (
	drv driver.Driver = &Driver{}

	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
	_ driver.Connector     = (*Connector)(nil)
	_ io.Closer            = (*Connector)(nil)
)

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	cfg, err := parseDSN(name)
	if err != nil {
		return nil, err
	}
	cc, err := newClientConn(cfg, nil)
	if err != nil {
		return nil, err
	}
	c, err := openStreamConn(context.Background(), cc, cc, cfg)
	if err != nil {
		_ = cc.Close()
		return nil, err
	}
	return c, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return NewConnector(name)
}

type Connector struct {
	Name string

	mu       sync.Mutex
	cfg      *dsnConfig
	dialOpts []grpc.DialOption
	cc       *grpc.ClientConn
	closed   bool
}

// NewConnector validates the DSN and returns a new Connector with optional custom gRPC DialOptions.
func NewConnector(name string, dialOpts ...grpc.DialOption) (*Connector, error) {
	cfg, err := parseDSN(name)
	if err != nil {
		return nil, err
	}
	return &Connector{
		Name:     name,
		cfg:      cfg,
		dialOpts: dialOpts,
	}, nil
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cc, cfg, err := c.getClientConn()
	if err != nil {
		return nil, err
	}
	return openStreamConn(ctx, cc, nil, cfg)
}

func (c *Connector) Driver() driver.Driver {
	return drv
}

func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	if c.cc == nil {
		return nil
	}
	err := c.cc.Close()
	c.cc = nil
	return err
}

func (c *Connector) getClientConn() (*grpc.ClientConn, *dsnConfig, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, errors.New("remote-ha: connector is closed")
	}
	if c.cfg == nil {
		cfg, err := parseDSN(c.Name)
		if err != nil {
			return nil, nil, err
		}
		c.cfg = cfg
	}
	if c.cc == nil {
		cc, err := newClientConn(c.cfg, c.dialOpts)
		if err != nil {
			return nil, nil, err
		}
		c.cc = cc
	}
	return c.cc, c.cfg, nil
}

type dsnConfig struct {
	host               string
	replicationID      string
	token              string
	useTLS             bool
	insecureSkipVerify bool
	keepaliveInterval  time.Duration
}

func parseDSN(name string) (*dsnConfig, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("remote-ha: empty DSN")
	}

	u, err := url.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("remote-ha: parse url: %w", err)
	}

	var useTLS bool
	switch u.Scheme {
	case "http", "grpc":
		useTLS = false
	case "https", "grpcs":
		useTLS = true
	case "":
		return nil, errors.New("remote-ha: missing URL scheme (expected http://, https://, grpc://, or grpcs://)")
	default:
		if u.Host == "" {
			return nil, errors.New("remote-ha: missing URL scheme (expected http://, https://, grpc://, or grpcs://)")
		}
		return nil, fmt.Errorf("remote-ha: unsupported URL scheme %q (expected http, https, grpc, or grpcs)", u.Scheme)
	}

	if u.Host == "" {
		return nil, errors.New("remote-ha: missing host in DSN")
	}

	cfg := &dsnConfig{
		host:              u.Host,
		replicationID:     strings.TrimPrefix(u.Path, "/"),
		useTLS:            useTLS,
		keepaliveInterval: defaultKeepaliveInterval,
	}

	if u.User != nil {
		if pass, ok := u.User.Password(); ok {
			cfg.token = pass
		} else if user := u.User.Username(); user != "" {
			cfg.token = user
		}
	}

	if u.RawQuery != "" {
		q, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return nil, fmt.Errorf("remote-ha: parse query params: %w", err)
		}

		if v := q.Get("replicationID"); v != "" && cfg.replicationID == "" {
			cfg.replicationID = v
		} else if v := q.Get("replID"); v != "" && cfg.replicationID == "" {
			cfg.replicationID = v
		}

		if v := q.Get("token"); v != "" {
			cfg.token = v
		} else if v := q.Get("grpcToken"); v != "" {
			cfg.token = v
		}

		if v := q.Get("insecure"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("remote-ha: invalid insecure param: %w", err)
			}
			cfg.useTLS = !b
		} else if v := q.Get("grpcInsecure"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("remote-ha: invalid grpcInsecure param: %w", err)
			}
			cfg.useTLS = !b
		}

		if v := q.Get("insecureSkipVerify"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("remote-ha: invalid insecureSkipVerify param: %w", err)
			}
			cfg.insecureSkipVerify = b
		}

		if v := q.Get("keepalive"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				return nil, fmt.Errorf("remote-ha: invalid keepalive duration %q", v)
			}
			cfg.keepaliveInterval = d
		}
	}

	return cfg, nil
}

func newClientConn(cfg *dsnConfig, extraOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	opts := make([]grpc.DialOption, 0, 2+len(extraOpts))
	if cfg.useTLS {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: cfg.insecureSkipVerify,
		})))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if cfg.token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(grpcCredentials{
			token:  cfg.token,
			useTLS: cfg.useTLS,
		}))
	}

	opts = append(opts, extraOpts...)

	cc, err := grpc.NewClient(cfg.host, opts...)
	if err != nil {
		return nil, fmt.Errorf("create grpc client: %w", err)
	}
	return cc, nil
}

func openStreamConn(dialCtx context.Context, cc *grpc.ClientConn, ownCC *grpc.ClientConn, cfg *dsnConfig) (driver.Conn, error) {
	client := sqlv1.NewDatabaseServiceClient(cc)

	// The stream's lifetime must outlive dialCtx (which database/sql uses only for dialing).
	streamCtx, cancel := context.WithCancel(context.Background())
	stopCancel := context.AfterFunc(dialCtx, cancel)

	queryStream, err := client.Query(streamCtx)
	if err != nil {
		stopCancel()
		cancel()
		return nil, fmt.Errorf("create query stream: %w", err)
	}
	if !stopCancel() {
		cancel()
		_ = queryStream.CloseSend()
		return nil, dialCtx.Err()
	}

	return newConn(cfg.replicationID, queryStream, streamCtx, cancel, ownCC, cfg.keepaliveInterval), nil
}

type grpcCredentials struct {
	token  string
	useTLS bool
}

func (c grpcCredentials) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.token,
	}, nil
}

func (c grpcCredentials) RequireTransportSecurity() bool {
	return c.useTLS
}
