# Relay protocol

<img src="logo.png" alt="Relay logo" width="120">

Relay uses JSON over HTTP. The default address is `http://127.0.0.1:8080`.

The API does not provide authentication or authorization. Bind it to a trusted interface and submit only commands that are safe to execute on worker hosts.

## Job submission

`POST /v1/jobs`

```json
{
  "type": "command",
  "payload": "echo hello",
  "priority": 5,
  "max_retries": 3,
  "timeout": "30s",
  "run_at": "2026-09-09T12:00:00Z"
}
```

`type` defaults to `command`. `payload` is required. `priority` defaults to zero. `max_retries` counts additional attempts after the initial attempt. `timeout` uses Go duration syntax. `run_at` is optional and must be RFC3339.

The response is `201 Created` with the complete job object.

## Job inspection

- `GET /v1/jobs` returns `{ "jobs": [...] }`
- `GET /v1/jobs/{id}` returns one job
- `GET /v1/jobs/{id}/history` returns `{ "events": [...] }`
- `POST /v1/jobs/{id}/cancel` marks queued or running work cancelled

Job statuses are `queued`, `running`, `completed`, `failed`, and `cancelled`.

## Worker registration

Register a worker:

`POST /v1/workers/register`

```json
{
  "id": "worker-1"
}
```

Send a heartbeat:

`POST /v1/workers/{id}/heartbeat`

Claim work:

`POST /v1/workers/{id}/claim`

The response is:

- `200 OK` with a job when work is available
- `204 No Content` when the queue has no ready job
- `404 Not Found` when the worker is not registered or is marked dead

List workers with `GET /v1/workers`. Each worker includes its ID, registration time, last heartbeat, and `healthy` or `dead` status.

## Result reporting

`POST /v1/jobs/{id}/result`

Successful result:

```json
{
  "worker_id": "worker-1",
  "success": true,
  "result": "hello"
}
```

Failed result:

```json
{
  "worker_id": "worker-1",
  "success": false,
  "result": "partial output",
  "error": "exit status 1"
}
```

The server accepts a result only from the worker that owns the running attempt. A stale result after timeout, cancellation, or worker recovery receives `409 Conflict`; workers ignore that response and continue polling.

## Statistics and metrics

- `GET /v1/stats` returns queue counts, worker counts, and metric values as JSON
- `GET /metrics` returns Prometheus-compatible text
- `GET /healthz` returns `{ "status": "ok" }`

## Dashboard and profiling

- `GET /dashboard/` serves the embedded operations dashboard
- the dashboard reads the same JSON and metrics endpoints as external clients
- pass `--pprof 127.0.0.1:6060` to expose Go profiling endpoints on a separate listener

The profiling listener is disabled by default.
