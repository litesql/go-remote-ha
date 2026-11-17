# go-remote-ha

## Overview

`go-remote-ha` is a Go `database/sql` driver designed for remote access to [SQLite HA databases](https://github.com/litesql/ha) over gRPC. This driver enables seamless interaction with high-availability SQLite databases using Go's standard library interface.

## Features
- **Remote Access**: Connect to SQLite HA databases over gRPC (`http`, `https`, `grpc`, `grpcs`).
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
<scheme>://[:<token>@]<host>:<port>[/<replicationID>][?<options>]
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

### Custom gRPC Dial Options
You can pass custom `grpc.DialOption`s via `NewConnector` and `sql.OpenDB`:

```go
connector, err := remoteha.NewConnector("https://db.example.com:8443/mydb", customDialOpts...)
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
