# Relay persistence

Relay persists the complete server state as one JSON document. The default path is `relay-state.json`; pass `--data ""` to disable persistence.

## Snapshot contents

The state file contains:

- every known job
- job status and attempt metadata
- retry and scheduling timestamps
- lifecycle history
- worker registration and heartbeat state

The file is intended to be readable for local development and inspection.

## Write path

For each state mutation, Relay:

1. serializes the complete state to a temporary file in the target directory
2. flushes the temporary file
3. closes the file
4. replaces the active state file

The temporary file uses the same directory so the replacement stays on the same filesystem. On Windows, replacing an existing file requires removing the previous path before the rename.

The server reports persistence errors to the caller instead of silently discarding them. The in-memory transition has already occurred when a later snapshot fails, so operators should treat a persistence error as an unhealthy server state.

## Startup recovery

Relay loads the snapshot before accepting requests. Jobs that were running when the previous process stopped are returned to the queue with their attempt count preserved. They may execute again because the previous worker may have completed the command before the server stopped.

Persisted workers are marked `dead` during startup. A worker becomes `healthy` again only after it registers or sends a heartbeat.

## Delivery semantics

Relay provides at-least-once execution. The server cannot know whether a worker completed a command when the connection failed before result reporting. Recovery can therefore produce duplicate command execution.

Commands should be idempotent when duplicate execution is unsafe. Use an application-level idempotency key in the command payload or downstream system.

## Inspecting state

The state file can be viewed with any JSON tool while the server is stopped. Use the API for live inspection so readers do not observe a file during replacement.

The persistence package is covered by round-trip and benchmark tests:

```bash
go test ./internal/persistence
go test -bench=. -benchmem ./internal/persistence
```
