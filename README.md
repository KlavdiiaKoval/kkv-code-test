
# Assignment: Queue between a file reader and writer

It’s your job to design a simple messaging system by using a queue service:

- Read lines from a file
- Write lines to a queue service via a network protocol
- Read lines from the queue service via a network protocol
- Write lines to a file

Implement it as 2 asynchronous workers exchanging information by using a service.

## Task requirements

- Documentation on how to run your solution
- Queuing service needs to be written with only stdlib of your language of choice
- An arbitrary ASCII text file fed into the solution should produce an identical copy

### Bonus Tasks

- Use only one process for the reader/writer and one for the queue service
- Your solution has full unit test coverage
- You have a service/container-based deployment strategy
- Documentation of design choices and alternatives
- Scaling strategy

---

## Solution Overview

File-to-file messaging pipeline using Go's standard library. Reads lines from a file (or directory), sends them through an HTTP queue service, and writes them to output files, verifying content reproduction. Only two processes are used: one for the queue service and one for the worker (reader/writer).

- **queue-service**: In-memory FIFO queue with HTTP API. Endpoints:
  - `POST /queues/:name/messages` (enqueue, accepts `application/octet-stream` or JSON `{ "message": "..." }`)
  - `DELETE /queues/:name/messages/head` (dequeue, returns message as `application/octet-stream`)

- **worker**: Single process that acts as both reader (producer) and writer (consumer). Supports single-file and directory watch. Communicates with the queue-service using the above endpoints.



---

## Diagrams

### Architecture Diagram

```mermaid
flowchart LR
    A[input.txt /data/in/*.txt] -- read lines --> W1([worker: producer])
    W1 -- HTTP POST /queues/:name/messages --> Q([queue-service])
    W2 -- HTTP DELETE /queues/:name/messages/head --> Q
    Q -- message --> W2
    W2 -- write lines --> B[output.txt /data/out/*.txt]
    subgraph Worker Process
      W1
      W2
    end
    subgraph Queue Service
      Q
    end
```

### Sequence Diagram

```mermaid
sequenceDiagram
  participant User
  participant Worker as Worker (producer/consumer)
  participant Queue as Queue Service
  participant Output as Output File

  User->>Worker: Drop file in data/in/
  Worker->>Queue: POST /queues/:name/messages (enqueue lines)
  Queue-->>Worker: 202 Accepted (or 400 Bad Request)
  Worker->>Queue: DELETE /queues/:name/messages/head (dequeue lines)
  Queue-->>Worker: 200 OK (message) or 204 No Content
  Worker->>Output: Write lines to data/out/
```

### Deployment Diagram

```mermaid
flowchart TD
  subgraph Docker Host
    subgraph Container: queue-service
      Q[queue-service]
    end
    subgraph Container: worker
      W[worker]
    end
    V[(data volume: ./data)]
    W -- mount /data --> V
    Q -- mount /data --> V
  end
  User -.-> V
```

### 1. Build and start all services

```bash
docker-compose up --build
```

This will start both the queue service and the worker, mounting the local `./data` directory into the containers.

### 2. Using the pipeline

- Create input/output directories if they don't exist:

  ```bash
  mkdir -p data/in data/out
  ```

- Drop a file into `data/in/` (for example):

  ```bash
  echo "hello world" > data/in/example.txt
  ```

- Or create a more complex file with line breaks:

  ```bash
  cat > data/in/complex.txt <<EOF
  first line
  second line
  third line

  last line
  EOF
  ```

- The worker will process them and write outputs to `data/out/` (mirroring filenames):

  ```bash
  cat data/out/example.txt
  # Output: hello world
  ```

### 3. Stopping services

```bash
docker-compose down
```

---

## Quick Start (Local, without Docker)

### 1. Start the queue service (terminal 1)

```bash
go run ./cmd/queue-service
```

### 2. Start the worker (terminal 2)

#### Directory watch (default)

```bash
mkdir -p data/in data/out
go run ./cmd/worker -watch-dir data/in -watch-out data/out -queue lines
```

Drop files into `data/in/` and outputs will appear in `data/out/` with the same filenames.

#### Single-file mode (default if -watch-dir not set)

```bash
go run ./cmd/worker -in data/input.txt -out data/output.txt -queue lines
```

- Make sure the queue service is running before starting the worker.
- The default queue URL is `http://localhost:8080` (use `-queue-url` if you change it).
- The `-queue` flag must match between producer and consumer.


#### Example: Creating a complex input file

```bash
cat > data/in/complex.txt <<EOF
first line
second line
third line

last line
EOF
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

### queue-service flags

- `-addr` - Server address (default: `:8080`)

### worker flags

- `-queue-url` - Queue service URL (default: `http://localhost:8080`)
- `-queue` - Queue name (default: `lines`)
- `-in` - Input file path (default: `data/input.txt`)
- `-out` - Output file path (default: `data/output.txt`)
- `-watch-dir` - Directory to watch for new files (disables `-in/-out`)
- `-watch-out` - Output directory for processed files (default: `data/out`)
- `-watch-interval` - Poll interval (default: 500ms)

## Design

- Only two processes: one for the queue service, one for the worker (reader/writer)
- Standard library only in queue service
- Output file is a byte-for-byte copy of input
- In-memory FIFO queue with mutex protection
- HTTP API:

  - `POST /queues/:name/messages` (enqueue, accepts `application/octet-stream` or JSON `{ "message": "..." }`)
  - `DELETE /queues/:name/messages/head` (dequeue, returns message as `application/octet-stream`, 204 if empty)

## API Reference

### Enqueue (POST /queues/:name/messages)

- **Content-Type:** `application/octet-stream` (raw bytes) or `application/json` (`{"message": "..."}`)
- **Response:**
  - `202 Accepted` on success
  - `400 Bad Request` for missing/invalid input

### Dequeue (DELETE /queues/:name/messages/head)

- **Response:**
  - `200 OK` with message as `application/octet-stream` if a message is available
  - `204 No Content` if the queue is empty
  - `400 Bad Request` for missing/invalid queue name
- Worker uses goroutines for producer/consumer logic

## Limitations

- Single in-memory queue service (messages lost on restart)
- No batching, retries, limits, or metrics
- Watch mode uses polling (not FS events)
- No back-pressure or max queue size enforcement

## Scaling Strategy

This solution was built for simplicity and local testing, but it can be scaled to handle higher throughput and improved reliability with a few key enhancements.

### Queue Service

- **Persistence:** Right now, the queue is in-memory, which means any restart results in lost messages. For production use, we'd need to back it with something persistent—like Redis, PostgreSQL, or a distributed message broker—to ensure data durability and allow for scaling across instances.

- **Stateless Architecture:** To scale the queue service horizontally, it should be stateless. That way, multiple instances can run behind a load balancer, all connected to the same backend queue.

- **Rate Limiting and Auth:** To avoid abuse or system overload, it’s important to add API rate limiting and authentication in front of the queue endpoints.

### Worker Scaling

- **Parallel Processing:** You can spin up multiple worker instances to consume and process messages in parallel, which helps increase throughput. Just make sure file writes are idempotent so you don’t end up with duplicate results.

- **Partitioning:** For larger datasets or high-volume use cases, consider partitioning input data or splitting up queues so different workers can handle different subsets. This helps distribute the workload more efficiently.

### Deployment and Operations

- **Container Orchestration:** e.g Kubernetes
- **Monitoring and Alerting:** It's essential to track system health. Tools like Prometheus and Grafana can monitor queue depth, worker status, and processing times. Set up alerts so issues like failures or growing backlogs don’t go unnoticed.

- **Back-pressure:** To avoid memory issues or system crashes under heavy load, implement back-pressure mechanisms or enforce queue size limits to keep things running smoothly.
