# go-remote-ha

## Overview

`go-remote-ha` is a Go database/sql driver designed for remote access to [SQLite HA databases](https://github.com/litesql/ha). This driver enables seamless interaction with high-availability SQLite databases, making it easier to build robust applications that require reliable data access.

## Features
- **Remote Access**: Connect to SQLite HA databases over the network.
- **Easy Integration**: Works with the standard `database/sql` package in Go.

## Installation
To install `go-remote-ha`, use the following command:

```bash
go get -u github.com/litesql/go-remote-ha
```

## Usage
Here’s a simple example of how to use `go-remote-ha`:

```go
package main

import (
    "database/sql"
    _ "github.com/litesql/go-remote-ha"
)

func main() {
    db, err := sql.Open("remote-ha", "your-dsn-here")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Your database operations here
}
```

## Embedded replicas

For embedded replicas, use [go-sqlite-ha](https://github.com/litesql/go-sqlite-ha) or [go-sqlite3-ha](https://github.com/litesql/go-sqlite3-ha) drivers.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


