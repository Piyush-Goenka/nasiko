# BalanceAI Production Runbook

## Strategy decision matrix

| Workload                                        | Recommended strategy | Why                                       |
|-------------------------------------------------|----------------------|-------------------------------------------|
| Stateless agents, uniform request cost          | `round_robin`        | Simplest, lowest overhead                 |
| Mixed-duration requests (translation, summary)  | `least_connections`  | Adapts to per-request cost                |
| Many routers, no shared state                   | `p2c`                | Near-optimal without coordination         |
| Multi-turn LLM chat with session context        | `chwbl`              | KV-cache affinity, 10-50x cheaper prefill |
| Bursty load with occasional slow tails          | any + hedging        | p99 dramatically improved                 |

## Configuration knobs (env vars)

| Var                                | Default                                              | Effect                                       |
|------------------------------------|------------------------------------------------------|----------------------------------------------|
| `BALANCER_DISCOVERY`               | auto (K8s if `K8S_ENABLED=true`, else docker)        | Discovery backend                            |
| `BALANCER_HEALTH_INTERVAL_S`        | 5                                                   | Active probe interval                        |
| `BALANCER_HEALTH_TIMEOUT_S`         | 2                                                   | Probe timeout                                |
| `BALANCER_CB_FAILURE_THRESHOLD`     | 5                                                   | Consecutive failures before circuit opens    |
| `BALANCER_CB_COOLDOWN_INITIAL_S`    | 10                                                  | Cooldown before half-open probe              |
| `BALANCER_CB_COOLDOWN_MAX_S`        | 300                                                 | Cap on exponential cooldown                  |
| `BALANCER_SLOW_START_WINDOW_S`      | 30                                                  | New-replica ramp window                      |
| `BALANCER_HEDGING_ENABLED`          | false                                               | Default for hedged requests                  |
| `PHOENIX_OTLP_ENDPOINT`             | http://phoenix-observability:4318/v1/traces         | OTel exporter target                         |

## Failure-mode playbooks

### Symptom: dashboard shows all replicas red

- Cause: all `/health` probes failing.
- Check: `curl http://<replica>:5000/health` directly. If 404, the agent
  doesn't expose `/health` and the A2A `/.well-known/agent-card` fallback
  should kick in within 5s.
- Action: `kubectl logs <pod>` or `docker logs <container>` to confirm the
  agent process is alive.

### Symptom: Gini > 0.3 with `round_robin` active

- Cause: strategy hot-swap didn't apply, OR a replica is circuit-open and
  being skipped.
- Check: `GET /balancer/pool/<agent>`, verify `strategy` field matches
  expectation, and all replicas are `SERVING`.
- Action: re-`PUT /balancer/strategy/<agent>`; check event log for
  `circuit_open` events.

### Symptom: new replica gets no traffic after deploy

- Cause: slow-start window not elapsed, OR active probe hasn't promoted
  DISCOVERED -> SERVING yet.
- Check: pool endpoint `status` field. Should walk DISCOVERED -> READY ->
  SERVING within ~10s.
- Action: wait one full health-probe interval. If stuck, check that
  `agents-net` (Docker) or label selector (K8s) sees the new replica.

### Symptom: Phoenix shows no balancer spans

- Cause: Phoenix exporter init failed silently at startup.
- Check: router logs at startup for `phoenix.otel.register failed` warning.
- Action: verify `phoenix-observability` container is up; verify
  `PHOENIX_OTLP_ENDPOINT` reachable from the router pod/container.

## SLOs

| SLO                                          | Target          |
|----------------------------------------------|-----------------|
| Route decision p99 latency                   | < 1 ms          |
| Time to eject sick replica (passive path)    | < 30 s          |
| Pool error rate (excluding ejected)          | < 1% over 30d   |
| Fairness Gini (steady state)                 | < 0.1           |

## Capacity sizing

| Pool size | Strategy overhead per request                          | Memory per pool |
|-----------|--------------------------------------------------------|-----------------|
| 5         | < 50 µs                                                | ~2 KB           |
| 50        | < 100 µs                                               | ~20 KB          |
| 500       | < 500 µs (CHWBL: ring rebuild dominates)               | ~250 KB         |

Beyond ~500 replicas per pool, shard by agent flavor.
