# Rate Limiter + Load Balancer (from raw sockets)

A reverse-proxy load balancer and per-client rate limiter, built directly on
TCP sockets — no `http.server`, no `requests`, no rate-limiting or
load-balancing libraries. Every layer of the stack (HTTP parsing, connection
routing, health checking, failover, token-bucket throttling, and the load
test client itself) is implemented from scratch in Python's standard
library only.

This exists because "design a rate limiter" and "design a load balancer"
are two of the most common backend/systems interview questions — this is
what happens when you actually build and load-test the thing instead of
just whiteboarding it.

## Architecture

```
                     ┌─────────────────────┐
   clients  ───────► │   Load Balancer      │
  (many src IPs)     │   - per-IP token      │
                      │     bucket rate limit │
                      │   - round robin /     │
                      │     least connections │
                      │   - active health      │
                      │     checks (0.5s)      │
                      │   - automatic failover │
                      └──────────┬────────────┘
                                 │
                 ┌───────────────┼───────────────┐
                 ▼               ▼               ▼
            Backend :8001   Backend :8002   Backend :8003
```

**`rate_limiter.py`** — Two rate-limiting algorithms implemented from
scratch:
- `TokenBucket`: O(1) memory per client, continuous refill, allows short
  bursts up to a configurable capacity.
- `SlidingWindowLog`: exact request counting over a trailing window — more
  accurate at the cost of O(limit) memory per client.

A `RateLimiterRegistry` lazily creates one independent limiter per client
key (source IP), so one noisy client can never starve another's budget.

**`backend_server.py`** — A minimal HTTP server on raw sockets. Simulates
real work with a randomized delay per request, and supports a `--flaky`
mode that randomly drops connections to test resilience.

**`load_balancer.py`** — The core piece:
- Parses raw HTTP requests off the wire and proxies them to a backend pool.
- Two routing strategies: round-robin and least-connections.
- A background thread actively health-checks every backend every 500ms.
- **Automatic failover**: if a chosen backend refuses or drops mid-request,
  the request is silently retried on the next healthy backend before the
  client ever sees an error.
- Per-client-IP rate limiting sits in front of routing, independent of
  which backend eventually serves the request.

**`load_test.py`** — A concurrent load-testing client (threads + raw
sockets). Each simulated "client" is bound to its own source IP in the
`127.0.0.0/8` loopback range, so the rate limiter sees genuinely distinct
clients rather than one shared IP — this matters, because testing a
per-client rate limiter from a single source IP just measures the wrong
thing. Reports throughput and p50/p95/p99 latency.

**`run_demo.py`** — Boots the full system as real subprocesses and runs a
3-phase scripted scenario (see results below).

## Running it

```bash
pip install --break-system-packages -r requirements.txt  # none needed, stdlib only
python3 run_demo.py
```

Or run components individually:
```bash
python3 backend_server.py --port 8001
python3 load_balancer.py --backends 127.0.0.1:8001,127.0.0.1:8002 --port 9000
python3 load_test.py --port 9000 --concurrency 40 --requests 25
```

## Results (measured on this machine, not estimated)

**Phase 1 — baseline, 3/3 backends healthy, 40 distinct simulated clients:**
- 1,000/1,000 requests succeeded (100%)
- 1,671 req/s throughput
- p50 20.8ms / p95 31.9ms / p99 36.0ms

**Phase 2 — one backend killed mid-fleet, load re-run immediately after:**
- 1,000/1,000 requests still succeeded (100%) — **zero client-visible
  errors** despite losing a third of the backend fleet
- Throughput held at 1,726 req/s
- The health-check loop detected the dead backend and the load balancer
  routed exclusively to the two survivors, entirely transparently to
  clients

**Phase 3 — rate limiter isolation test:**
- One client firing 100 rapid requests: 41% allowed, 59% correctly
  throttled with `429 Too Many Requests`, all within its own token bucket
- Simultaneously, 20 other well-behaved clients: 100/100 requests
  succeeded, completely unaffected — proving the limiter is per-client,
  not a global choke point
