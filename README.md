# Relay

Relay is a distributed job queue and worker system written in Go.

The server accepts command jobs over HTTP, stores them in an in-memory priority queue, and assigns them to independent workers. Workers execute the command payload on their host and report the result to the server.

## Current capabilities

- submit jobs through the HTTP API or command line
- prioritize queued jobs
- inspect job status, output, and errors
- cancel queued jobs or mark running jobs cancelled
- register multiple workers
- distribute jobs across concurrent workers
- execute command payloads using the host operating system's shell
- protect shared queue state with synchronization
- verify queue and worker behavior with unit, integration, and race tests

## Architecture

```text
client or CLI
      |
      v
HTTP server
      |
      v
in-memory priority queue
      |
      +------------+------------+
      v            v            v
  worker-1     worker-2     worker-3
```

Jobs move through these states:

```text
queued -> running -> completed
                 \-> failed
queued or running -> cancelled
```

Higher priority values are claimed first. Jobs with equal priority are claimed in creation order.

## Quick start

Start the server:

```bash
go run ./cmd/relay server
```

Start one or more workers in separate terminals:

```bash
go run ./cmd/relay worker --id worker-1
go run ./cmd/relay worker --id worker-2
```

Submit a command:

```bash
go run ./cmd/relay submit "echo hello"
```

Inspect jobs and workers:

```bash
go run ./cmd/relay jobs
go run ./cmd/relay job <id>
go run ./cmd/relay workers
```

Cancel a job:

```bash
go run ./cmd/relay cancel <id>
```

The server listens on `127.0.0.1:8080` by default. Use `--server` with client and worker commands when the server uses another address.

## Command line interface

### Start the server

```text
relay server [--listen address]
```

### Start a worker

```text
relay worker [--server url] [--id id] [--poll duration]
```

The default worker ID combines the host name and process ID. Set `--id` when running multiple workers on the same host.

### Submit a job

```text
relay submit [--server url] [--type type] [--priority number] command
```

The command can contain spaces when passed as one quoted argument. The command's combined standard output and standard error are returned as the job result.

### Inspect and cancel jobs

```text
relay jobs [--server url]
relay job [--server url] <id>
relay cancel [--server url] <id>
relay workers [--server url]
```

## HTTP API

- `GET /healthz` checks server availability
- `POST /v1/jobs` creates a job
- `GET /v1/jobs` lists jobs
- `GET /v1/jobs/{id}` returns one job
- `POST /v1/jobs/{id}/cancel` cancels a job
- `POST /v1/workers/register` registers a worker
- `GET /v1/workers` lists workers
- `POST /v1/workers/{id}/claim` claims the next queued job
- `POST /v1/jobs/{id}/result` reports a worker result

Create a job:

```json
{
  "type": "command",
  "payload": "echo hello",
  "priority": 1
}
```

Report a successful result:

```json
{
  "worker_id": "worker-1",
  "success": true,
  "result": "hello"
}
```

## Operational boundaries

- job state is held in server memory and is lost when the server stops
- workers execute submitted commands with the permissions of their local process
- cancelling a running job changes its recorded state but does not interrupt the command already running on the worker
- the current worker loop polls the server when no job is available

Only submit commands that are trusted to run on the worker host.

## Development

Format the source:

```bash
gofmt -w cmd internal
```

Run tests:

```bash
go test ./...
```

Run the race detector:

```bash
go test -race ./...
```

Run static checks:

```bash
go vet ./...
```

The repository uses the MIT License.
