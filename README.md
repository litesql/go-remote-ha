# go-remote-ha

## Overview

`go-remote-ha` is a Go `database/sql` driver designed for remote access to [SQLite HA databases](https://github.com/litesql/ha) over gRPC. This driver enables seamless interaction with high-availability SQLite databases using Go's standard library interface.

## Features
- **Remote Access**: Connect to SQLite HA databases over gRPC (`http`, `https`, `grpc`, `grpcs`).
- **Client-Side Load Balancing**: Distribute pooled `database/sql` streams across multiple gRPC endpoints or DNS-resolved backends (`round_robin`, `least_request`, `pick_first`).
- **Connection Multiplexing**: Shares a single underlying gRPC connection across pooled `database/sql` streams.
- **Easy Integration**: Works directly with Go's standard `database/sql` package.

## Installation
To install `go-remote-ha`, use the following command:

```bash
go get -u github.com/litesql/go-remote-ha
```

## Usage
Here’s an example of how to use `go-remote-ha`:

```go
package main

import (
	"database/sql"

	_ "github.com/litesql/go-remote-ha"
)

func main() {
	db, err := sql.Open("remote-ha", "https://:your-secret-token@db.example.com:8443/my-replication-id")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Your database operations here
}
```

## DSN Format

```text
<scheme>://[:<token>@]<host1>:<port1>[,<host2>:<port2>,...][/<replicationID>][?<options>]
```

### Schemes
- `https://` or `grpcs://`: Enables TLS transport security.
- `http://` or `grpc://`: Uses insecure plaintext transport (suitable for local development or trusted networks).

### Authentication
Pass the authentication token in any of the following ways:
- Password in userinfo: `https://:my-token@host:8443/mydb` or `https://user:my-token@host:8443/mydb`
- Username in userinfo: `https://my-token@host:8443/mydb`
- Query parameter: `https://host:8443/mydb?token=my-token` (or `grpcToken=my-token`)

### Query Parameters
| Parameter | Description | Default |
|---|---|---|
| `replicationID` / `replID` | Target database replication ID (if not provided in the URL path) | `""` |
| `token` / `grpcToken` | Authorization token sent in gRPC metadata | `""` |
| `insecure` / `grpcInsecure` | Disable TLS (`true`/`false`) | Depends on scheme |
| `insecureSkipVerify` | Skip TLS certificate verification (`true`/`false`) | `false` |
| `keepalive` | Stream keepalive ping interval (`0s` disables application pings) | `25s` |
| `loadBalancingPolicy` / `loadBalancer` / `lbPolicy` / `lb` | gRPC client load balancing policy (`round_robin`, `least_request`, `pick_first`) | `round_robin` when multiple hosts are given; gRPC default otherwise |
| `resolver` | gRPC name resolver scheme for single-target DSNs (e.g. `dns`, `passthrough`, `xds`) | gRPC default (`dns`) |
| `authority` / `serverName` | Override HTTP/2 `:authority` and TLS `ServerName` (useful when dialing IPs with a shared TLS certificate) | Endpoint host |

### Client-Side Load Balancing

#### Multiple Static Endpoints
Provide a comma-separated list of `host:port` endpoints in the DSN. When multiple hosts are specified, `round_robin` load balancing is enabled automatically:

```go
db, err := sql.Open("remote-ha", "https://:your-secret-token@node1.example.com:8443,node2.example.com:8443,node3.example.com:8443/mydb")
```

To use active-passive failover (`pick_first`) or `least_request` instead:

```go
db, err := sql.Open("remote-ha", "https://:your-secret-token@node1.example.com:8443,node2.example.com:8443/mydb?loadBalancer=least_request")
```

#### DNS Service Discovery (e.g. Kubernetes Headless Services)
For a single hostname that resolves to multiple A/AAAA records, enable `round_robin` or `least_request` via query parameter:

```go
db, err := sql.Open("remote-ha", "https://:your-secret-token@db-headless.default.svc.cluster.local:8443/mydb?loadBalancer=round_robin")
```

### Custom gRPC Dial Options
You can pass custom `grpc.DialOption`s (including `remoteha.WithLoadBalancingPolicy`) via `NewConnector` and `sql.OpenDB`:

```go
connector, err := remoteha.NewConnector(
	"https://db.example.com:8443/mydb",
	remoteha.WithLoadBalancingPolicy(remoteha.LoadBalancerRoundRobin),
)
if err != nil {
	panic(err)
}
db := sql.OpenDB(connector)
defer db.Close()
```

## Embedded replicas

For embedded replicas, use [go-sqlite-ha](https://github.com/litesql/go-sqlite-ha) or [go-sqlite3-ha](https://github.com/litesql/go-sqlite3-ha) drivers.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
