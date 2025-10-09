
# kkv-code-test

File-to-file messaging pipeline using Go's standard library. Reads lines from a file (or directory in watch mode), sends them through an HTTP queue service, and writes them to output files, verifying content reproduction. Only two processes are used: one for the queue service and one for the worker (reader/writer).



## Components

- **queue-service**: In-memory FIFO queue with HTTP API (stdlib only)
- **worker**: Single process that acts as both reader (producer) and writer (consumer). Supports single-file, stream, and directory watch modes.



### Flow Diagram

```
input.txt -> [worker] -> [queue-service] -> [worker] -> output.txt
```


## Quick Start

### 1. Start the queue service (terminal 1):
```bash
go run ./cmd/queue-service
```

### 2. Start the worker (terminal 2):

#### Directory watch mode (recommended):
```bash
mkdir -p data/in data/out
go run ./cmd/worker -watch-dir data/in -watch-out data/out -queue lines
```
Drop files into `data/in/` and outputs will appear in `data/out/` with the same filenames.

#### Single-file once mode:
```bash
go run ./cmd/worker -mode once -in data/input.txt -out data/output.txt
```

#### Stream (tail) mode:
```bash
go run ./cmd/worker -mode stream -in data/input.txt -out data/output.txt
echo "another line" >> data/input.txt
```


## Testing

Run all tests:
```bash
go test ./...
```
With race detector:
```bash
go test -race ./...
```
Coverage:
```bash
go test -cover ./...
```
HTML report:
```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```


## Configuration

### queue-service flags:
- `-addr` - Server address (default: `:8080`)

### worker flags:
- `-queue-url` - Queue service URL (default: `http://localhost:8080`)
- `-queue` - Queue name (default: `lines`)
- `-in` - Input file path (default: `data/input.txt`)
- `-out` - Output file path (default: `data/output.txt`)
- `-mode` - `once` (default) or `stream` (tail mode)
- `-watch-dir` - Directory to watch for new files (disables `-in/-out/-mode`)
- `-watch-out` - Output directory for processed files (default: `data/out`)
- `-watch-interval` - Poll interval (default: 500ms)



## Design

- Only two processes: one for the queue service, one for the worker (reader/writer)
- Standard library only in queue service
- Output file is a byte-for-byte copy of input
- In-memory FIFO queue with mutex protection
- HTTP API: `POST /queues/{name}` (enqueue), `DELETE /queues/{name}` (dequeue), `HEAD /queues/{name}` (queue length)
- Worker uses goroutines for producer/consumer logic



## Limitations

- Single in-memory queue service (messages lost on restart)
- No batching, retries, limits, or metrics
- Watch mode uses polling (not FS events)
- Stream mode buffers a partial trailing line until newline appended
- No back-pressure or max queue size enforcement
