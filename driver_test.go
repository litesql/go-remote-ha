package remoteha

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	haconnect "github.com/litesql/go-ha/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type mockDatabaseServer struct {
	sqlv1.UnimplementedDatabaseServiceServer

	expectedToken string
	pingCount     atomic.Int64
	streamCount   atomic.Int64

	mu      sync.Mutex
	queries []string
	handler func(ctx context.Context, req *sqlv1.QueryRequest) (*sqlv1.QueryResponse, error)
}

func (s *mockDatabaseServer) recordSQL(q string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, q)
}

func (s *mockDatabaseServer) recordedSQL() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.queries))
	copy(out, s.queries)
	return out
}

func (s *mockDatabaseServer) Query(stream grpc.BidiStreamingServer[sqlv1.QueryRequest, sqlv1.QueryResponse]) error {
	if s.expectedToken != "" {
		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}
		vals := md.Get("authorization")
		if len(vals) == 0 || vals[0] != s.expectedToken {
			return status.Error(codes.Unauthenticated, "invalid token")
		}
	}

	s.streamCount.Add(1)

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if req.GetType() == sqlv1.QueryType_QUERY_TYPE_PING {
			s.pingCount.Add(1)
			if err := stream.Send(&sqlv1.QueryResponse{}); err != nil {
				return err
			}
			continue
		}

		s.recordSQL(req.GetSql())

		if s.handler != nil {
			resp, err := s.handler(stream.Context(), req)
			if err != nil {
				return err
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			continue
		}

		if err := stream.Send(&sqlv1.QueryResponse{}); err != nil {
			return err
		}
	}
}

func startMockServer(t *testing.T, srv *mockDatabaseServer) (*bufconn.Listener, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcSrv := grpc.NewServer()
	sqlv1.RegisterDatabaseServiceServer(grpcSrv, srv)

	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	cleanup := func() {
		grpcSrv.Stop()
		_ = lis.Close()
	}
	t.Cleanup(cleanup)
	return lis, cleanup
}

func openTestDB(t *testing.T, dsn string, lis *bufconn.Listener) *sql.DB {
	t.Helper()
	connector, err := NewConnector(dsn, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
	if err != nil {
		t.Fatalf("NewConnector(%q) failed: %v", dsn, err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name         string
		dsn          string
		wantErr      bool
		wantHost     string
		wantHosts    []string
		wantRepl     string
		wantToken    string
		wantTLS      bool
		wantSkipV    bool
		wantKA       time.Duration
		wantLB       string
		wantResolver string
		wantAuth     string
	}{
		{
			name:      "http insecure with path",
			dsn:       "http://localhost:8080/mydb",
			wantHost:  "localhost:8080",
			wantHosts: []string{"localhost:8080"},
			wantRepl:  "mydb",
			wantTLS:   false,
			wantKA:    defaultKeepaliveInterval,
		},
		{
			name:      "https with colon password token",
			dsn:       "https://:secret-token@db.example.com:443/prod",
			wantHost:  "db.example.com:443",
			wantHosts: []string{"db.example.com:443"},
			wantRepl:  "prod",
			wantToken: "secret-token",
			wantTLS:   true,
			wantKA:    defaultKeepaliveInterval,
		},
		{
			name:      "https with user-only token",
			dsn:       "https://bare-token@db.example.com:443/prod",
			wantHost:  "db.example.com:443",
			wantHosts: []string{"db.example.com:443"},
			wantRepl:  "prod",
			wantToken: "bare-token",
			wantTLS:   true,
			wantKA:    defaultKeepaliveInterval,
		},
		{
			name:      "grpcs with query options",
			dsn:       "grpcs://db.example.com:9090?replID=shard1&token=qtoken&insecureSkipVerify=true&keepalive=10s",
			wantHost:  "db.example.com:9090",
			wantHosts: []string{"db.example.com:9090"},
			wantRepl:  "shard1",
			wantToken: "qtoken",
			wantTLS:   true,
			wantSkipV: true,
			wantKA:    10 * time.Second,
		},
		{
			name:      "https overridden with insecure=true and keepalive=0",
			dsn:       "https://localhost:8443/test?insecure=true&keepalive=0s",
			wantHost:  "localhost:8443",
			wantHosts: []string{"localhost:8443"},
			wantRepl:  "test",
			wantTLS:   false,
			wantKA:    0,
		},
		{
			name:      "multi-host defaults to round_robin and deduplicates hosts",
			dsn:       "https://:my-secret@node1.example.com:8443,node2.example.com:8443,node1.example.com:8443/mydb",
			wantHost:  "node1.example.com:8443,node2.example.com:8443",
			wantHosts: []string{"node1.example.com:8443", "node2.example.com:8443"},
			wantRepl:  "mydb",
			wantToken: "my-secret",
			wantTLS:   true,
			wantKA:    defaultKeepaliveInterval,
			wantLB:    LoadBalancerRoundRobin,
		},
		{
			name:      "multi-host ipv6 with least_request and serverName",
			dsn:       "grpcs://[::1]:8443,[::1]:8444/clusterdb?loadBalancer=least_request&serverName=cluster.internal",
			wantHost:  "[::1]:8443,[::1]:8444",
			wantHosts: []string{"[::1]:8443", "[::1]:8444"},
			wantRepl:  "clusterdb",
			wantTLS:   true,
			wantKA:    defaultKeepaliveInterval,
			wantLB:    LoadBalancerLeastRequest,
			wantAuth:  "cluster.internal",
		},
		{
			name:         "single host with dns resolver and round_robin",
			dsn:          "grpc://db-headless.default.svc:8080/mydb?resolver=dns&loadBalancingPolicy=roundrobin",
			wantHost:     "db-headless.default.svc:8080",
			wantHosts:    []string{"db-headless.default.svc:8080"},
			wantRepl:     "mydb",
			wantTLS:      false,
			wantKA:       defaultKeepaliveInterval,
			wantLB:       LoadBalancerRoundRobin,
			wantResolver: "dns",
		},
		{
			name:      "multi-host explicit pick_first",
			dsn:       "http://node1:8080,node2:8080/mydb?lb=pick-first",
			wantHost:  "node1:8080,node2:8080",
			wantHosts: []string{"node1:8080", "node2:8080"},
			wantRepl:  "mydb",
			wantKA:    defaultKeepaliveInterval,
			wantLB:    LoadBalancerPickFirst,
		},
		{
			name:    "empty dsn",
			dsn:     "",
			wantErr: true,
		},
		{
			name:    "missing scheme host:port",
			dsn:     "localhost:8080/mydb",
			wantErr: true,
		},
		{
			name:    "unsupported scheme",
			dsn:     "postgres://localhost:5432/mydb",
			wantErr: true,
		},
		{
			name:    "missing host",
			dsn:     "http:///mydb",
			wantErr: true,
		},
		{
			name:    "empty host in multi-host list",
			dsn:     "http://node1:8080,/mydb",
			wantErr: true,
		},
		{
			name:    "invalid port in multi-host list",
			dsn:     "http://node1:8080,node2:notaport/mydb",
			wantErr: true,
		},
		{
			name:    "unsupported load balancer policy",
			dsn:     "http://localhost:8080/mydb?loadBalancer=nonexistent_policy",
			wantErr: true,
		},
		{
			name:    "resolver combined with multi-host",
			dsn:     "http://node1:8080,node2:8080/mydb?resolver=dns",
			wantErr: true,
		},
		{
			name:    "invalid resolver scheme",
			dsn:     "http://localhost:8080/mydb?resolver=123bad!",
			wantErr: true,
		},
		{
			name:    "invalid insecure bool",
			dsn:     "http://localhost:8080/mydb?insecure=notabool",
			wantErr: true,
		},
		{
			name:    "invalid keepalive duration",
			dsn:     "http://localhost:8080/mydb?keepalive=-5s",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseDSN(tc.dsn)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for DSN %q, got nil", tc.dsn)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for DSN %q: %v", tc.dsn, err)
			}
			if cfg.host != tc.wantHost {
				t.Errorf("host = %q, want %q", cfg.host, tc.wantHost)
			}
			if !slices.Equal(cfg.hosts, tc.wantHosts) {
				t.Errorf("hosts = %v, want %v", cfg.hosts, tc.wantHosts)
			}
			if cfg.replicationID != tc.wantRepl {
				t.Errorf("replicationID = %q, want %q", cfg.replicationID, tc.wantRepl)
			}
			if cfg.token != tc.wantToken {
				t.Errorf("token = %q, want %q", cfg.token, tc.wantToken)
			}
			if cfg.useTLS != tc.wantTLS {
				t.Errorf("useTLS = %v, want %v", cfg.useTLS, tc.wantTLS)
			}
			if cfg.insecureSkipVerify != tc.wantSkipV {
				t.Errorf("insecureSkipVerify = %v, want %v", cfg.insecureSkipVerify, tc.wantSkipV)
			}
			if cfg.keepaliveInterval != tc.wantKA {
				t.Errorf("keepaliveInterval = %v, want %v", cfg.keepaliveInterval, tc.wantKA)
			}
			if cfg.loadBalancingPolicy != tc.wantLB {
				t.Errorf("loadBalancingPolicy = %q, want %q", cfg.loadBalancingPolicy, tc.wantLB)
			}
			if cfg.resolver != tc.wantResolver {
				t.Errorf("resolver = %q, want %q", cfg.resolver, tc.wantResolver)
			}
			if cfg.authority != tc.wantAuth {
				t.Errorf("authority = %q, want %q", cfg.authority, tc.wantAuth)
			}
		})
	}

	if _, err := sql.Open("remote-ha", "localhost:8080/missing-scheme"); err == nil {
		t.Fatalf("expected sql.Open to fail on invalid DSN")
	}
}

func TestExecAndLastInsertId(t *testing.T) {
	var gotReq *sqlv1.QueryRequest
	srv := &mockDatabaseServer{
		expectedToken: "my-secret",
		handler: func(ctx context.Context, req *sqlv1.QueryRequest) (*sqlv1.QueryResponse, error) {
			gotReq = req
			return &sqlv1.QueryResponse{
				LastInsertId: 42,
				RowsAffected: 3,
			}, nil
		},
	}
	lis, _ := startMockServer(t, srv)
	db := openTestDB(t, "http://my-secret@127.0.0.1:8080/appdb", lis)

	bigUint := uint64(math.MaxInt64) + 99
	res, err := db.ExecContext(context.Background(), "INSERT INTO items(name, seq, note) VALUES (?, ?, ?)",
		"widget", bigUint, sql.NullString{Valid: false})
	if err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	lastID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId error: %v", err)
	}
	if lastID != 42 {
		t.Errorf("LastInsertId = %d, want 42", lastID)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected error: %v", err)
	}
	if affected != 3 {
		t.Errorf("RowsAffected = %d, want 3", affected)
	}

	if gotReq.GetReplicationId() != "appdb" {
		t.Errorf("ReplicationId = %q, want %q", gotReq.GetReplicationId(), "appdb")
	}
	if len(gotReq.GetParams()) != 3 {
		t.Fatalf("len(Params) = %d, want 3", len(gotReq.GetParams()))
	}
	if got := haconnect.FromAnypb(gotReq.GetParams()[1].GetValue()); got != bigUint {
		t.Errorf("uint64 param = %v (%T), want %v", got, got, bigUint)
	}
}

func TestQueryAndTypeNormalization(t *testing.T) {
	now := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	bigUint := uint64(math.MaxInt64) + 500

	srv := &mockDatabaseServer{
		handler: func(ctx context.Context, req *sqlv1.QueryRequest) (*sqlv1.QueryResponse, error) {
			vInt32, _ := anypb.New(wrapperspb.Int32(12))
			vUint32, _ := anypb.New(wrapperspb.UInt32(34))
			vUint64Small, _ := anypb.New(wrapperspb.UInt64(56))
			vUint64Big, _ := anypb.New(wrapperspb.UInt64(bigUint))
			vFloat32, _ := anypb.New(wrapperspb.Float(1.5))
			vStr, _ := anypb.New(wrapperspb.String("hello"))
			vBool, _ := anypb.New(wrapperspb.Bool(true))
			vBytes, _ := anypb.New(wrapperspb.Bytes([]byte("raw")))
			vTime, _ := anypb.New(timestamppb.New(now))
			vNull, _ := anypb.New(&emptypb.Empty{})

			return &sqlv1.QueryResponse{
				ResultSet: &sqlv1.Data{
					Columns: []string{"i32", "u32", "u64_small", "u64_big", "f32", "str", "b", "bin", "ts", "nil_col"},
					Rows: []*sqlv1.Row{
						{
							Values: []*anypb.Any{vInt32, vUint32, vUint64Small, vUint64Big, vFloat32, vStr, vBool, vBytes, vTime, vNull},
						},
					},
				},
			}, nil
		},
	}
	lis, _ := startMockServer(t, srv)
	db := openTestDB(t, "http://127.0.0.1:8080/mydb", lis)

	var (
		i32      int64
		u32      int64
		u64Small uint64
		u64Big   uint64
		f32      float64
		str      string
		b        bool
		bin      []byte
		ts       time.Time
		nilCol   sql.NullString
	)

	err := db.QueryRowContext(context.Background(), "SELECT * FROM all_types").
		Scan(&i32, &u32, &u64Small, &u64Big, &f32, &str, &b, &bin, &ts, &nilCol)
	if err != nil {
		t.Fatalf("QueryRowContext Scan failed: %v", err)
	}

	if i32 != 12 || u32 != 34 || u64Small != 56 || u64Big != bigUint {
		t.Errorf("integer mismatch: i32=%d u32=%d u64Small=%d u64Big=%d", i32, u32, u64Small, u64Big)
	}
	if f32 != 1.5 || str != "hello" || !b || string(bin) != "raw" || !ts.Equal(now) || nilCol.Valid {
		t.Errorf("value mismatch: f32=%v str=%q b=%v bin=%q ts=%v nilCol=%+v", f32, str, b, bin, ts, nilCol)
	}
}

func TestRowsNextBoundsCheck(t *testing.T) {
	v1, _ := anypb.New(wrapperspb.Int64(1))
	v2, _ := anypb.New(wrapperspb.Int64(2))
	r := &rows{
		data: &sqlv1.Data{
			Columns: []string{"only_one"},
			Rows: []*sqlv1.Row{
				{Values: []*anypb.Any{v1, v2}},
			},
		},
	}
	dest := make([]driver.Value, 1)
	if err := r.Next(dest); err == nil {
		t.Fatalf("expected error when row has more values than dest slice, got nil")
	}
}

func TestConnectCtxDoesNotKillPooledStream(t *testing.T) {
	srv := &mockDatabaseServer{}
	lis, _ := startMockServer(t, srv)
	db := openTestDB(t, "http://127.0.0.1:8080/mydb", lis)
	db.SetMaxOpenConns(1)

	firstCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := db.PingContext(firstCtx); err != nil {
		cancel()
		t.Fatalf("initial PingContext failed: %v", err)
	}
	// Cancel the context used during initial Connect!
	cancel()

	// Subsequent operations must reuse the same healthy stream without opening a second stream.
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("second PingContext failed after canceling initial ctx: %v", err)
	}
	if got := srv.streamCount.Load(); got != 1 {
		t.Errorf("streamCount = %d, want 1 (pooled stream should have survived)", got)
	}
}

func TestQueryContextCancellationDoesNotDesyncPool(t *testing.T) {
	srv := &mockDatabaseServer{
		handler: func(ctx context.Context, req *sqlv1.QueryRequest) (*sqlv1.QueryResponse, error) {
			if strings.Contains(req.GetSql(), "SLOW") {
				select {
				case <-time.After(200 * time.Millisecond):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				v, _ := anypb.New(wrapperspb.String("stale-slow-result"))
				return &sqlv1.QueryResponse{
					ResultSet: &sqlv1.Data{
						Columns: []string{"val"},
						Rows:    []*sqlv1.Row{{Values: []*anypb.Any{v}}},
					},
				}, nil
			}
			v, _ := anypb.New(wrapperspb.String("fresh-fast-result"))
			return &sqlv1.QueryResponse{
				ResultSet: &sqlv1.Data{
					Columns: []string{"val"},
					Rows:    []*sqlv1.Row{{Values: []*anypb.Any{v}}},
				},
			}, nil
		},
	}
	lis, _ := startMockServer(t, srv)
	db := openTestDB(t, "http://127.0.0.1:8080/mydb", lis)
	db.SetMaxOpenConns(1)

	slowCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	var s string
	err := db.QueryRowContext(slowCtx, "SELECT 'SLOW'").Scan(&s)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// Next query must NOT receive "stale-slow-result"; the canceled stream must be replaced.
	err = db.QueryRowContext(context.Background(), "SELECT 'FAST'").Scan(&s)
	if err != nil {
		t.Fatalf("subsequent fast query failed: %v", err)
	}
	if s != "fresh-fast-result" {
		t.Fatalf("got %q, want %q", s, "fresh-fast-result")
	}
	if got := srv.streamCount.Load(); got < 2 {
		t.Errorf("expected broken stream to be replaced, streamCount = %d", got)
	}
}

func TestKeepaliveNoDeadlock(t *testing.T) {
	srv := &mockDatabaseServer{}
	lis, _ := startMockServer(t, srv)
	db := openTestDB(t, "http://127.0.0.1:8080/mydb?keepalive=35ms", lis)
	db.SetMaxOpenConns(1)

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("initial PingContext failed: %v", err)
	}

	// Let connection sit idle past multiple keepalive intervals.
	time.Sleep(150 * time.Millisecond)

	if pings := srv.pingCount.Load(); pings < 2 {
		t.Fatalf("expected at least 2 keepalive pings, got %d", pings)
	}

	// Connection must remain usable (no deadlock).
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("PingContext after idle keepalive failed: %v", err)
	}
}

func TestTransactionsAndCanceledTxRollback(t *testing.T) {
	srv := &mockDatabaseServer{}
	lis, _ := startMockServer(t, srv)
	db := openTestDB(t, "http://127.0.0.1:8080/mydb", lis)
	db.SetMaxOpenConns(1)

	// Unsupported TxOptions must fail per driver.ConnBeginTx contract.
	if _, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err == nil {
		t.Fatalf("expected error for unsupported isolation level")
	}
	if _, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true}); err == nil {
		t.Fatalf("expected error for unsupported read-only transaction")
	}

	// Commit flow.
	tx1, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx failed: %v", err)
	}
	if _, err := tx1.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("tx.Exec failed: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	// Context cancellation on idle Tx must still send ROLLBACK to the server!
	txCtx, cancel := context.WithCancel(context.Background())
	tx2, err := db.BeginTx(txCtx, nil)
	if err != nil {
		cancel()
		t.Fatalf("BeginTx 2 failed: %v", err)
	}
	if _, err := tx2.ExecContext(txCtx, "INSERT INTO t VALUES (2)"); err != nil {
		cancel()
		t.Fatalf("tx2.ExecContext failed: %v", err)
	}

	cancel() // Triggers database/sql's automatic Rollback in background.

	deadline := time.Now().Add(2 * time.Second)
	foundRollback := false
	for time.Now().Before(deadline) {
		for _, q := range srv.recordedSQL() {
			if q == "ROLLBACK" {
				foundRollback = true
				break
			}
		}
		if foundRollback {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !foundRollback {
		t.Fatalf("expected ROLLBACK to be sent to server after tx context cancellation; recorded=%v", srv.recordedSQL())
	}
}

func TestPingEvictsBrokenStreamAndConcurrentUsage(t *testing.T) {
	var failNext atomic.Bool
	srv := &mockDatabaseServer{
		handler: func(ctx context.Context, req *sqlv1.QueryRequest) (*sqlv1.QueryResponse, error) {
			if failNext.CompareAndSwap(true, false) {
				return nil, status.Error(codes.Unavailable, "simulated stream drop")
			}
			v, _ := anypb.New(wrapperspb.Int64(1))
			return &sqlv1.QueryResponse{
				RowsAffected: 1,
				ResultSet: &sqlv1.Data{
					Columns: []string{"one"},
					Rows:    []*sqlv1.Row{{Values: []*anypb.Any{v}}},
				},
			}, nil
		},
	}
	lis, _ := startMockServer(t, srv)
	db := openTestDB(t, "http://127.0.0.1:8080/mydb?keepalive=50ms", lis)
	db.SetMaxOpenConns(4)

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("initial PingContext failed: %v", err)
	}

	// Simulate abrupt server-side stream termination during Ping:
	// PingContext must return driver.ErrBadConn so database/sql evicts the broken stream from the pool.
	beforeStreams := srv.streamCount.Load()
	failNext.Store(true)
	err := db.PingContext(context.Background())
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("expected PingContext error to wrap driver.ErrBadConn, got: %v", err)
	}
	// Because the bad connection was evicted, the next PingContext opens a fresh stream and succeeds.
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("subsequent PingContext should succeed on fresh stream, got: %v", err)
	}
	if got := srv.streamCount.Load(); got <= beforeStreams {
		t.Fatalf("expected new stream after eviction, before=%d after=%d", beforeStreams, got)
	}

	// Simulate abrupt stream drop on a pooled connection during QueryRowContext:
	// database/sql's retry loop transparently evicts the bad conn and succeeds on the same call!
	failNext.Store(true)
	var val int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&val); err != nil {
		t.Fatalf("QueryRowContext should transparently retry on driver.ErrBadConn, got: %v", err)
	}

	// Stress concurrent queries, prepared statements, and transactions under -race.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				var n int64
				if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&n); err != nil {
					t.Errorf("concurrent QueryRow failed: %v", err)
					return
				}
				stmt, err := db.PrepareContext(context.Background(), "UPDATE t SET a = ?")
				if err != nil {
					t.Errorf("concurrent Prepare failed: %v", err)
					return
				}
				_, err = stmt.ExecContext(context.Background(), j)
				_ = stmt.Close()
				if err != nil {
					t.Errorf("concurrent Stmt.Exec failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

type mockCluster struct {
	mu        sync.RWMutex
	listeners map[string]*bufconn.Listener
	stops     map[string]func()
}

func newMockCluster() *mockCluster {
	return &mockCluster{
		listeners: make(map[string]*bufconn.Listener),
		stops:     make(map[string]func()),
	}
}

func (mc *mockCluster) addNode(t *testing.T, addr string, srv *mockDatabaseServer, serverOpts ...grpc.ServerOption) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcSrv := grpc.NewServer(serverOpts...)
	sqlv1.RegisterDatabaseServiceServer(grpcSrv, srv)

	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	var stopOnce sync.Once
	stopFn := func() {
		stopOnce.Do(func() {
			mc.mu.Lock()
			delete(mc.listeners, addr)
			mc.mu.Unlock()
			grpcSrv.Stop()
			_ = lis.Close()
		})
	}
	t.Cleanup(stopFn)

	mc.mu.Lock()
	mc.listeners[addr] = lis
	mc.stops[addr] = stopFn
	mc.mu.Unlock()
}

func (mc *mockCluster) stopNode(addr string) {
	mc.mu.RLock()
	stopFn := mc.stops[addr]
	mc.mu.RUnlock()
	if stopFn != nil {
		stopFn()
	}
}

func (mc *mockCluster) dialOption() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		mc.mu.RLock()
		lis, ok := mc.listeners[addr]
		mc.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("connection refused: %s", addr)
		}
		return lis.DialContext(ctx)
	})
}

func TestMultiHostRoundRobinLoadBalancing(t *testing.T) {
	cluster := newMockCluster()
	srv1 := &mockDatabaseServer{expectedToken: "lb-secret"}
	srv2 := &mockDatabaseServer{expectedToken: "lb-secret"}
	srv3 := &mockDatabaseServer{expectedToken: "lb-secret"}

	cluster.addNode(t, "node1:8080", srv1)
	cluster.addNode(t, "node2:8080", srv2)
	cluster.addNode(t, "node3:8080", srv3)

	connector, err := NewConnector(
		"http://:lb-secret@node1:8080,node2:8080,node3:8080/mydb",
		cluster.dialOption(),
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxIdleConns(0) // Force every Ping to open a new stream through the gRPC picker.

	// Wait until all 3 subchannels finish connecting and enter the round_robin ready set.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.PingContext(context.Background()); err != nil {
			t.Fatalf("warmup PingContext failed: %v", err)
		}
		if srv1.streamCount.Load() > 0 && srv2.streamCount.Load() > 0 && srv3.streamCount.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if srv1.streamCount.Load() == 0 || srv2.streamCount.Load() == 0 || srv3.streamCount.Load() == 0 {
		t.Fatalf("not all backends became ready: counts = [%d, %d, %d]",
			srv1.streamCount.Load(), srv2.streamCount.Load(), srv3.streamCount.Load())
	}

	base1 := srv1.streamCount.Load()
	base2 := srv2.streamCount.Load()
	base3 := srv3.streamCount.Load()

	// Once all 3 subchannels are Ready, 9 consecutive streams must hit each server exactly 3 times.
	for i := 0; i < 9; i++ {
		if err := db.PingContext(context.Background()); err != nil {
			t.Fatalf("PingContext %d failed: %v", i, err)
		}
	}

	got1 := srv1.streamCount.Load() - base1
	got2 := srv2.streamCount.Load() - base2
	got3 := srv3.streamCount.Load() - base3
	if got1 != 3 || got2 != 3 || got3 != 3 {
		t.Errorf("round_robin stream delta = [%d, %d, %d], want [3, 3, 3]", got1, got2, got3)
	}
}

func TestMultiHostFailoverWhenNodeDown(t *testing.T) {
	cluster := newMockCluster()
	makeSrv := func(name string) *mockDatabaseServer {
		return &mockDatabaseServer{
			handler: func(ctx context.Context, req *sqlv1.QueryRequest) (*sqlv1.QueryResponse, error) {
				v, _ := anypb.New(wrapperspb.String(name))
				return &sqlv1.QueryResponse{
					ResultSet: &sqlv1.Data{
						Columns: []string{"node"},
						Rows:    []*sqlv1.Row{{Values: []*anypb.Any{v}}},
					},
				}, nil
			},
		}
	}

	srv1 := makeSrv("node1")
	srv2 := makeSrv("node2")
	cluster.addNode(t, "node1:8080", srv1)
	cluster.addNode(t, "node2:8080", srv2)

	// First endpoint "dead-node:8080" is completely unreachable from the start.
	connector, err := NewConnector(
		"http://dead-node:8080,node1:8080,node2:8080/mydb",
		cluster.dialOption(),
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	// Open 4 concurrent connections so both node1 and node2 hold active pooled streams.
	conns := make([]*sql.Conn, 4)
	for i := range conns {
		c, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("db.Conn(%d) failed despite healthy nodes: %v", i, err)
		}
		if err := c.PingContext(context.Background()); err != nil {
			t.Fatalf("conn.PingContext(%d) failed: %v", i, err)
		}
		conns[i] = c
	}
	for _, c := range conns {
		_ = c.Close()
	}

	// Shut down node1 abruptly; database/sql must evict broken streams and route traffic to node2.
	cluster.stopNode("node1:8080")

	for i := 0; i < 8; i++ {
		var node string
		if err := db.QueryRowContext(context.Background(), "SELECT node").Scan(&node); err != nil {
			t.Fatalf("QueryRowContext(%d) after node1 shutdown failed: %v", i, err)
		}
		if node != "node2" {
			t.Fatalf("got response from %q after node1 shutdown, want node2", node)
		}
	}
}

func TestMultiHostPickFirstAndFailover(t *testing.T) {
	cluster := newMockCluster()
	srv1 := &mockDatabaseServer{}
	srv2 := &mockDatabaseServer{}
	cluster.addNode(t, "node1:8080", srv1)
	cluster.addNode(t, "node2:8080", srv2)

	connector, err := NewConnector(
		"http://node1:8080,node2:8080/mydb?loadBalancer=pick_first",
		cluster.dialOption(),
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxIdleConns(0)

	for i := 0; i < 5; i++ {
		if err := db.PingContext(context.Background()); err != nil {
			t.Fatalf("PingContext(%d) failed: %v", i, err)
		}
	}

	if got1, got2 := srv1.streamCount.Load(), srv2.streamCount.Load(); got1 != 5 || got2 != 0 {
		t.Fatalf("pick_first counts = [%d, %d], want [5, 0]", got1, got2)
	}

	// Stop primary node1; pick_first must fail over to node2.
	cluster.stopNode("node1:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if err := db.PingContext(ctx); err != nil {
			t.Fatalf("PingContext(%d) after failover failed: %v", i, err)
		}
	}
	if got2 := srv2.streamCount.Load(); got2 != 3 {
		t.Fatalf("srv2 streamCount after failover = %d, want 3", got2)
	}
}

func TestLeastRequestAndDialOptionLoadBalancing(t *testing.T) {
	cluster := newMockCluster()
	srv1 := &mockDatabaseServer{}
	srv2 := &mockDatabaseServer{}
	// Also verifies default port :80 normalization when port is omitted on http:// hosts.
	cluster.addNode(t, "node1:80", srv1)
	cluster.addNode(t, "node2:80", srv2)

	connector, err := NewConnector(
		"http://node1,node2/mydb?loadBalancer=least_request",
		cluster.dialOption(),
	)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxIdleConns(0)

	// Hold one stream busy on whichever node is picked first, then open additional streams:
	// least_request_experimental must prefer the idle backend.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.PingContext(context.Background()); err != nil {
			t.Fatalf("PingContext failed: %v", err)
		}
		if srv1.streamCount.Load() > 0 && srv2.streamCount.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if srv1.streamCount.Load() == 0 || srv2.streamCount.Load() == 0 {
		t.Fatalf("expected least_request to distribute across both backends, got [%d, %d]",
			srv1.streamCount.Load(), srv2.streamCount.Load())
	}

	// Verify programmatic WithLoadBalancingPolicy helper option as well.
	connectorOpt, err := NewConnector(
		"http://node1,node2/mydb?loadBalancer=pick_first",
		cluster.dialOption(),
		WithLoadBalancingPolicy(LoadBalancerRoundRobin),
	)
	if err != nil {
		t.Fatalf("NewConnector with WithLoadBalancingPolicy failed: %v", err)
	}
	dbOpt := sql.OpenDB(connectorOpt)
	t.Cleanup(func() { _ = dbOpt.Close() })
	dbOpt.SetMaxIdleConns(0)

	before1, before2 := srv1.streamCount.Load(), srv2.streamCount.Load()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := dbOpt.PingContext(context.Background()); err != nil {
			t.Fatalf("dbOpt PingContext failed: %v", err)
		}
		if srv1.streamCount.Load() > before1 && srv2.streamCount.Load() > before2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if srv1.streamCount.Load() == before1 || srv2.streamCount.Load() == before2 {
		t.Fatalf("expected WithLoadBalancingPolicy(LoadBalancerRoundRobin) override to hit both nodes")
	}
}

func generateTestCAAndCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, dnsNames ...string) tls.Certificate {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &priv.PublicKey, caKey)
	if err != nil {
		t.Fatalf("CreateCertificate failed: %v", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
}

func TestMultiHostTLSVerificationAndAuthorityOverride(t *testing.T) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("CA GenerateKey failed: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("CA CreateCertificate failed: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("ParseCertificate failed: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	t.Run("distinct per-endpoint DNS certificates", func(t *testing.T) {
		cluster := newMockCluster()
		srv1 := &mockDatabaseServer{expectedToken: "tls-tok"}
		srv2 := &mockDatabaseServer{expectedToken: "tls-tok"}

		cert1 := generateTestCAAndCert(t, caCert, caKey, "node1.example.com")
		cert2 := generateTestCAAndCert(t, caCert, caKey, "node2.example.com")

		cluster.addNode(t, "node1.example.com:8443", srv1, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert1}})))
		cluster.addNode(t, "node2.example.com:8443", srv2, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert2}})))

		connector, err := NewConnector(
			"grpcs://:tls-tok@node1.example.com:8443,node2.example.com:8443/mydb",
			cluster.dialOption(),
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: pool})),
		)
		if err != nil {
			t.Fatalf("NewConnector failed: %v", err)
		}
		db := sql.OpenDB(connector)
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxIdleConns(0)

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := db.PingContext(context.Background()); err != nil {
				t.Fatalf("TLS PingContext failed: %v", err)
			}
			if srv1.streamCount.Load() > 0 && srv2.streamCount.Load() > 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if srv1.streamCount.Load() == 0 || srv2.streamCount.Load() == 0 {
			t.Fatalf("expected TLS round_robin across both nodes, got [%d, %d]",
				srv1.streamCount.Load(), srv2.streamCount.Load())
		}
	})

	t.Run("shared certificate with serverName override on IP endpoints", func(t *testing.T) {
		cluster := newMockCluster()
		srv1 := &mockDatabaseServer{}
		srv2 := &mockDatabaseServer{}

		sharedCert := generateTestCAAndCert(t, caCert, caKey, "cluster.example.com")
		cluster.addNode(t, "10.0.0.1:8443", srv1, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{sharedCert}})))
		cluster.addNode(t, "10.0.0.2:8443", srv2, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{sharedCert}})))

		connector, err := NewConnector(
			"https://10.0.0.1:8443,10.0.0.2:8443/mydb?serverName=cluster.example.com",
			cluster.dialOption(),
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: pool})),
		)
		if err != nil {
			t.Fatalf("NewConnector failed: %v", err)
		}
		db := sql.OpenDB(connector)
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxIdleConns(0)

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := db.PingContext(context.Background()); err != nil {
				t.Fatalf("TLS PingContext with serverName override failed: %v", err)
			}
			if srv1.streamCount.Load() > 0 && srv2.streamCount.Load() > 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if srv1.streamCount.Load() == 0 || srv2.streamCount.Load() == 0 {
			t.Fatalf("expected TLS round_robin with serverName override across both IPs, got [%d, %d]",
				srv1.streamCount.Load(), srv2.streamCount.Load())
		}
	})
}
