package remoteha

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	_ "google.golang.org/grpc/balancer/leastrequest" // Register least_request_experimental balancer.
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
)

const (
	defaultKeepaliveInterval = 25 * time.Second
	multiHostScheme          = "remoteha-static"

	// LoadBalancerPickFirst connects to the first reachable endpoint and fails over on disconnect.
	LoadBalancerPickFirst = "pick_first"
	// LoadBalancerRoundRobin distributes new RPC streams evenly across all healthy endpoints.
	LoadBalancerRoundRobin = "round_robin"
	// LoadBalancerLeastRequest routes new RPC streams to the healthy endpoint with the fewest active streams.
	LoadBalancerLeastRequest = "least_request_experimental"
)

// WithLoadBalancingPolicy returns a grpc.DialOption that configures the default
// gRPC client-side load balancing policy (for example, LoadBalancerRoundRobin,
// LoadBalancerLeastRequest, or LoadBalancerPickFirst).
func WithLoadBalancingPolicy(policy string) grpc.DialOption {
	normalized := normalizeLoadBalancingPolicy(strings.TrimSpace(policy))
	return grpc.WithDefaultServiceConfig(buildLoadBalancingServiceConfig(normalized))
}

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
	scheme              string
	host                string
	hosts               []string
	replicationID       string
	token               string
	useTLS              bool
	insecureSkipVerify  bool
	keepaliveInterval   time.Duration
	loadBalancingPolicy string
	resolver            string
	authority           string
}

func parseDSN(name string) (*dsnConfig, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return nil, errors.New("remote-ha: empty DSN")
	}

	rawScheme, rest, ok := strings.Cut(trimmed, "://")
	if !ok || rawScheme == "" {
		return nil, errors.New("remote-ha: missing URL scheme (expected http://, https://, grpc://, or grpcs://)")
	}
	scheme := strings.ToLower(rawScheme)

	var useTLS bool
	switch scheme {
	case "http", "grpc":
		useTLS = false
	case "https", "grpcs":
		useTLS = true
	default:
		return nil, fmt.Errorf("remote-ha: unsupported URL scheme %q (expected http, https, grpc, or grpcs)", rawScheme)
	}

	var authorityPart, tail string
	if idx := strings.IndexAny(rest, "/?#"); idx >= 0 {
		authorityPart = rest[:idx]
		tail = rest[idx:]
	} else {
		authorityPart = rest
	}

	var userPrefix, rawHosts string
	if atIdx := strings.LastIndex(authorityPart, "@"); atIdx >= 0 {
		userPrefix = authorityPart[:atIdx+1]
		rawHosts = authorityPart[atIdx+1:]
	} else {
		rawHosts = authorityPart
	}

	if strings.TrimSpace(rawHosts) == "" {
		return nil, errors.New("remote-ha: missing host in DSN")
	}

	hosts, err := parseHosts(scheme, rawHosts)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(scheme + "://" + userPrefix + "localhost" + tail)
	if err != nil {
		return nil, fmt.Errorf("remote-ha: parse url: %w", err)
	}

	cfg := &dsnConfig{
		scheme:            scheme,
		host:              strings.Join(hosts, ","),
		hosts:             hosts,
		replicationID:     strings.TrimPrefix(u.Path, "/"),
		useTLS:            useTLS,
		keepaliveInterval: defaultKeepaliveInterval,
	}
	if len(hosts) > 1 {
		cfg.loadBalancingPolicy = LoadBalancerRoundRobin
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

		if v := firstNonEmpty(q, "loadBalancingPolicy", "loadBalancer", "lbPolicy", "lb"); v != "" {
			policy := normalizeLoadBalancingPolicy(v)
			if balancer.Get(policy) == nil {
				return nil, fmt.Errorf("remote-ha: unsupported load balancer %q", v)
			}
			cfg.loadBalancingPolicy = policy
		}

		if v := strings.TrimSpace(q.Get("resolver")); v != "" {
			if len(cfg.hosts) > 1 {
				return nil, errors.New("remote-ha: resolver query param cannot be combined with multiple hosts in DSN")
			}
			if !isValidScheme(v) {
				return nil, fmt.Errorf("remote-ha: invalid resolver scheme %q", v)
			}
			cfg.resolver = strings.ToLower(v)
		}

		if v := firstNonEmpty(q, "authority", "serverName"); v != "" {
			cfg.authority = v
		}
	}

	return cfg, nil
}

func parseHosts(scheme, rawHosts string) ([]string, error) {
	parts := strings.Split(rawHosts, ",")
	hosts := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		h := strings.TrimSpace(part)
		if h == "" {
			return nil, errors.New("remote-ha: empty host in DSN")
		}
		uHost, err := url.Parse(scheme + "://" + h)
		if err != nil {
			return nil, fmt.Errorf("remote-ha: invalid host %q: %w", h, err)
		}
		if uHost.User != nil || uHost.Path != "" || uHost.RawQuery != "" || uHost.Fragment != "" || uHost.Hostname() == "" {
			return nil, fmt.Errorf("remote-ha: invalid host %q", h)
		}
		if _, exists := seen[uHost.Host]; !exists {
			seen[uHost.Host] = struct{}{}
			hosts = append(hosts, uHost.Host)
		}
	}
	return hosts, nil
}

func firstNonEmpty(q url.Values, keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(q.Get(k)); v != "" {
			return v
		}
	}
	return ""
}

func normalizeLoadBalancingPolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "round_robin", "roundrobin", "round-robin":
		return LoadBalancerRoundRobin
	case "pick_first", "pickfirst", "pick-first":
		return LoadBalancerPickFirst
	case "least_request", "leastrequest", "least-request", "least_request_experimental":
		return LoadBalancerLeastRequest
	default:
		return strings.TrimSpace(policy)
	}
}

func buildLoadBalancingServiceConfig(policy string) string {
	payload := map[string]any{
		"loadBalancingConfig": []map[string]any{
			{policy: struct{}{}},
		},
	}
	b, _ := json.Marshal(payload)
	return string(b)
}

func isValidScheme(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isAlpha := ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
		isDigit := '0' <= c && c <= '9'
		if i == 0 && !isAlpha {
			return false
		}
		if !isAlpha && !isDigit && c != '+' && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

func normalizeEndpointAddr(host, scheme string) string {
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	defaultPort := "443"
	if scheme == "http" {
		defaultPort = "80"
	}
	bareHost := strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	return net.JoinHostPort(bareHost, defaultPort)
}

type multiHostResolverBuilder struct {
	addrs     []resolver.Address
	endpoints []resolver.Endpoint
}

func newMultiHostResolverBuilder(hosts []string, scheme string) *multiHostResolverBuilder {
	addrs := make([]resolver.Address, 0, len(hosts))
	endpoints := make([]resolver.Endpoint, 0, len(hosts))
	for _, h := range hosts {
		addr := normalizeEndpointAddr(h, scheme)
		ra := resolver.Address{
			Addr:       addr,
			ServerName: addr,
		}
		addrs = append(addrs, ra)
		endpoints = append(endpoints, resolver.Endpoint{
			Addresses: []resolver.Address{ra},
		})
	}
	return &multiHostResolverBuilder{
		addrs:     addrs,
		endpoints: endpoints,
	}
}

func (b *multiHostResolverBuilder) Scheme() string {
	return multiHostScheme
}

func (b *multiHostResolverBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	_ = cc.UpdateState(resolver.State{
		Addresses: b.addrs,
		Endpoints: b.endpoints,
	})
	return nopResolver{}, nil
}

type nopResolver struct{}

func (nopResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (nopResolver) Close()                                {}

func newClientConn(cfg *dsnConfig, extraOpts []grpc.DialOption) (*grpc.ClientConn, error) {
	opts := make([]grpc.DialOption, 0, 5+len(extraOpts))
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

	if cfg.authority != "" {
		opts = append(opts, grpc.WithAuthority(cfg.authority))
	}

	if cfg.loadBalancingPolicy != "" {
		opts = append(opts, WithLoadBalancingPolicy(cfg.loadBalancingPolicy))
	}

	target := cfg.host
	if len(cfg.hosts) > 1 {
		rb := newMultiHostResolverBuilder(cfg.hosts, cfg.scheme)
		opts = append(opts, grpc.WithResolvers(rb))
		target = multiHostScheme + ":///" + normalizeEndpointAddr(cfg.hosts[0], cfg.scheme)
	} else if len(cfg.hosts) == 1 {
		target = cfg.hosts[0]
		if cfg.resolver != "" {
			target = cfg.resolver + ":///" + target
		}
	}

	opts = append(opts, extraOpts...)

	cc, err := grpc.NewClient(target, opts...)
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
