# gedis

A Redis reimplementation in Go, compatible with the RESP2 protocol.

## Features

- Hash maps, sets, and sorted sets
- Lists and queues
- Transactions
- Pub/Sub messaging
- Geospatial indexing with geohash support
- Master-slave replication
- Pure Go implementation using only standard libraries

## Installation

```bash
go build -o gedis
```

## Usage

Start a standalone server:

```bash
./gedis --host 0.0.0.0 --port 6379
```

Start as a replica:

```bash
./gedis --replicaof <master_host>:<master_port>
```

## Project Structure

- `app/` - Main application entry point
- `gedis/` - Core database implementation
- `resp/` - RESP protocol parser and client
- `data/` - Data structures (sets, lists, geospatial index)
- `server/` - TCP server implementation
- `util/` - Utility functions

## Testing

```bash
go test ./...
```

## Requirements

- Go 1.24.5 or higher
