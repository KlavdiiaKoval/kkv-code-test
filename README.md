# corti-kkv

A simple file-to-file messaging pipeline using Go's standard library. Reads lines from a file, sends them through an HTTP queue service, and writes them to an output file.

## Components

- **queue-service**: In-memory FIFO queue with HTTP API
- **reader-writer**: Client that reads from input file, sends to queue, receives from queue, and writes to output file

## Quick Start

### Local Development
1. Start the queue service:
```bash
go run ./cmd/queue-service
```

2. Run the reader-writer (in another terminal):
```bash
echo "Hello World" > input.txt
go run ./cmd/reader-writer
cat output.txt  # Should show "Hello World"
```

### Using Docker
1. Create input file:
```bash
mkdir -p data
echo "Hello World" > data/input.txt
```

2. Run with Docker Compose:
```bash
docker compose up --build
```

3. Check output:
```bash
cat data/output.txt  # Should show "Hello World"
```

## Testing
```bash
go test ./...
```

## Configuration

### queue-service flags:
- `-addr` - Server address (default: `:8080`)

### reader-writer flags:
- `-in` - Input file path (default: `input.txt`)
- `-out` - Output file path (default: `output.txt`)  
- `-queue-url` - Queue service URL (default: `http://localhost:8080`)
- `-queue` - Queue name (default: `lines`)
