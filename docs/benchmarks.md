# Relay benchmarks

Relay includes repeatable Go benchmarks and a concurrent HTTP load generator. Results depend on the machine, Go version, filesystem, command payload, and server configuration, so this document describes the procedure rather than claiming a universal performance number.

## Queue and persistence benchmarks

Run the queue, HTTP, and persistence benchmarks:

```bash
go test -bench '^Benchmark' -benchmem ./internal/queue ./internal/server ./internal/persistence
```

The benchmark names cover:

- enqueue, claim, and complete throughput
- concurrent queue claims
- worker-loss recovery
- HTTP job submission
- JSON state snapshot writes and loads

Run one benchmark with a longer sample:

```bash
go test -bench '^BenchmarkConcurrentClaims$' -benchmem -benchtime=10s ./internal/queue
```

## HTTP load generator

Start a server:

```bash
go run ./cmd/relay server --data ""
```

Run concurrent submissions from another terminal:

```bash
go run ./cmd/relay-load \
  -server http://127.0.0.1:8080 \
  -clients 10 \
  -requests 1000 \
  -command "echo benchmark"
```

The load generator reports total requests, completed submissions, failures, elapsed time, throughput, and average submission latency. It measures submission and server persistence, not command execution completion.

## Profiling

Collect a CPU profile for queue operations:

```bash
go test -bench '^BenchmarkEnqueueClaimComplete$' \
  -cpuprofile=queue-cpu.out ./internal/queue
go tool pprof queue-cpu.out
```

Collect a memory profile:

```bash
go test -bench '^BenchmarkStateSave$' \
  -memprofile=state-memory.out ./internal/persistence
go tool pprof state-memory.out
```

For a running server, enable the optional profiling listener:

```bash
go run ./cmd/relay server --pprof 127.0.0.1:6060
go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30
```

Keep the profiling listener on a trusted interface.

## Local baseline

The following baseline was captured on September 8, 2026 on Windows amd64 with Go 1.27 and an AMD Ryzen 7 5800X:

```text
BenchmarkEnqueueClaimComplete  900792  1640 ns/op  2095 B/op  16 allocs/op
BenchmarkConcurrentClaims       10000  178656 ns/op  1624 B/op  13 allocs/op
BenchmarkWorkerRecovery         10000  179660 ns/op  2448 B/op  16 allocs/op
BenchmarkHTTPJobSubmission      10000  104535 ns/op  10187 B/op  105 allocs/op
BenchmarkStateSave                409  2861478 ns/op  44069 B/op  14 allocs/op
BenchmarkStateLoad               4576  261865 ns/op  93566 B/op  245 allocs/op
```

A local load run with four clients and 100 submissions per client completed 400 submissions with zero failures, 14,814 requests per second, and 263 microseconds average latency. These values describe one local run and are not a performance guarantee.

## Recording a local baseline

Record the following with the command, date, Go version, operating system, and server flags:

```text
Benchmark:
Environment:
Configuration:
Iterations:
ns/op:
allocs/op:
bytes/op:
throughput:
average latency:
errors:
```

Do not compare results from different configurations as if they were the same workload.
