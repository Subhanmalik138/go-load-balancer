# go-load-balancer

An HTTP reverse-proxy load balancer in Go. It sits in front of a pool of
backends, spreads requests across them with a pluggable strategy, and keeps
dead backends out of rotation until they recover.

## Features

- **Three selection strategies**
  - `round-robin` — even distribution across alive backends
  - `least-connections` — sends each request to the backend with the fewest
    in-flight requests
  - `weighted-round-robin` — smooth weighted round-robin (the nginx
    algorithm), so a weight-3 backend gets exactly 3 of every 4 requests
    without burstiness
- **Active health checks** — periodic probes mark backends down and bring
  them back automatically when they recover; no restart needed
- **Failover** — if a backend errors mid-request, the balancer retries the
  next alive backend before giving up (502 only when nothing is healthy)
- **Live stats** — `GET /__lb/stats` reports strategy, per-backend health,
  in-flight requests, and request/error counters as JSON
- **Two ways to run** — a JSON config file, or flags for a quick test
- **Graceful shutdown** — SIGINT/SIGTERM drains in-flight requests

## Quick start

```bash
# Two backends, round-robin (the default)
go run ./cmd/lb -backends http://localhost:9001,http://localhost:9002

# Weighted traffic: 3/4 of requests to 9001
go run ./cmd/lb -backends http://localhost:9001,http://localhost:9002 \
  -strategy weighted-round-robin -listen :8080
```

Or with a config file (see `config.example.json`):

```bash
go run ./cmd/lb -config config.example.json
```

Config file format:

```json
{
  "listen": ":8080",
  "strategy": "weighted-round-robin",
  "backends": [
    { "url": "http://localhost:9001", "weight": 3 },
    { "url": "http://localhost:9002", "weight": 1 }
  ],
  "health_check": { "path": "/health", "interval": "10s", "timeout": "2s" }
}
```

Then check the pool:

```bash
curl localhost:8080/__lb/stats
```

## Design decisions

1. **Strategies are a small interface, not a switch statement.**
   `Strategy.Next([]*Backend)` keeps selection logic isolated and testable,
   so a new algorithm (consistent hashing, latency-aware) is one new type.
2. **Health state lives on the backend, behind atomics.** Selection reads
   `alive` and in-flight counts without a lock, so the hot path never
   blocks on the health checker; only the weighted algorithm's running
   total needs a per-backend mutex.
3. **Failover retries without committing a response.** The proxy's error
   handler only records the failure — it never writes to the client — so a
   retry against the next backend can still produce a clean response.
   A backend that fails is marked down immediately (passive detection)
   and revived by the next passing health probe.
4. **Requests are cloned per attempt.** `httputil.ReverseProxy` rewrites
   the request URL in place; cloning keeps a failed attempt's rewrite from
   leaking into the retry.

## Running the tests

```bash
go test ./...
go vet ./...
```

## Tech stack

- Go 1.23, standard library only — no third-party dependencies
- `net/http/httputil` reverse proxy, `sync/atomic` for lock-free counters

## License

MIT
