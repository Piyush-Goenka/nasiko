# BalanceAI

Pluggable, health-aware, observable load balancer for Nasiko agent pools.

Sits between Nasiko's `RouterOrchestrator` and the agent pool. Auto-discovers
replicas via Docker SDK or the Kubernetes API. Distributes per-request using
swappable strategies. Heals automatically via a circuit breaker fed by active
`/health` probes and passive 5xx observation. Surfaces everything in a real-time
dashboard, Prometheus metrics, and Phoenix-native OTel spans.

---

## Quick start

```bash
# 1. Scale the translator agent to 3 replicas
docker compose -f docker-compose.local.yml --env-file .nasiko-local.env \
  up -d --scale a2a-translator=3

# 2. Hit the router, watch the balancer distribute
for i in $(seq 1 300); do
  curl -s "http://localhost:9100/router/route?query=translate hello" >/dev/null
done

# 3. Snapshot the pool (fairness Gini should be < 0.1)
curl -s http://localhost:8081/balancer/pools | jq

# 4. See the full demo (scale -> split -> kill -> recover -> hot-swap)
./agent-gateway/router/scripts/demo.sh
```

## Endpoints

| Route                              | Method | Returns                                          |
|------------------------------------|--------|--------------------------------------------------|
| `/balancer/pools`                  | GET    | All pools + fairness Gini                        |
| `/balancer/pool/{agent_name}`      | GET    | Per-replica state for one agent                  |
| `/balancer/strategy/{agent_name}`  | PUT    | Hot-swap strategy `{"strategy": "p2c"}`          |
| `/balancer/events`                 | GET    | SSE stream of replica lifecycle events           |
| `/balancer/metrics`                | GET    | Prometheus exposition                            |

## Strategies

| Strategy            | When to use                                         | Notes                                        |
|---------------------|-----------------------------------------------------|----------------------------------------------|
| `round_robin`        | Stateless agents, uniform request cost              | Atomic counter, lock-free                    |
| `random`             | Many routers with no shared state                   | secrets.choice; uniform                      |
| `least_connections`  | Mixed-duration requests (translation, summary)      | Slow-start gated to avoid cold-replica herd  |
| `p2c`                | Default for most agents                              | Near-optimal max load (Mitzenmacher 2001)    |
| `chwbl`              | Multi-turn LLM chat (KV-cache locality)             | Session-affinity via consistent hash + bound |

Strategy is swappable live:

```bash
curl -X PUT http://localhost:8081/balancer/strategy/translator \
  -H 'Content-Type: application/json' \
  -d '{"strategy": "p2c"}'
```

## Architecture

```
   existing Nasiko                          new (this module)
+---------------+   +-------------------+   +------------------------+   +---------+
|  Kong         |-> |  nasiko-router    |-> |  AgentClient           |-> | agent   |
|  :9100        |   |  /router (POST)   |   |   .send_request()      |   | replica |
+---------------+   +-------------------+   |    [PATCHED]           |   +---------+
                                            |     -> LoadBalancer    |
                                            |          .pick(name)   |
                                            |     -> httpx.post()    |
                                            +------------------------+
                                                       ^
                                                       | uses
                                            +------------------------+
                                            | InstanceRegistry       |   <- DiscoveryAdapter
                                            | HealthChecker          |   <- /health + passive
                                            | CircuitBreaker (per r) |
                                            | SlowStart              |
                                            | EventBroker            |   -> /balancer/events (SSE)
                                            | Metrics/Tracing        |   -> Prometheus + Phoenix
                                            +------------------------+
```

## Why BalanceAI vs Nginx / Kong / Envoy / Consul / HAProxy

| Capability                                  | Nginx | Kong   | Envoy   | Consul-Conn | HAProxy | **BalanceAI** |
|---------------------------------------------|:-----:|:------:|:-------:|:-----------:|:-------:|:-------------:|
| Round-Robin, Least-Conn, Random              | yes   | yes    | yes     | yes         | yes     | yes           |
| Power-of-Two-Choices (P2C)                   | no    | no     | partial | no          | yes     | yes           |
| Hedged requests                              | no    | no     | yes     | no          | no      | yes           |
| Circuit breaker + half-open single probe     | partial| no    | yes     | no          | partial | yes           |
| Slow start ramp                              | yes   | no     | yes     | no          | yes     | yes           |
| **KV-cache-aware (CHWBL)**                   | no    | no     | no      | no          | no      | **yes**       |
| **A2A `/.well-known/agent-card` fallback**   | no    | no     | no      | no          | no      | **yes**       |
| **Phoenix-native span attributes**           | no    | no     | no      | no          | no      | **yes**       |
| Docker + Kubernetes discovery                 | no    | yes    | yes     | yes         | no      | yes           |
| Hot strategy swap (no restart)               | no    | partial| yes     | no          | no      | yes           |

**Niche:** Generic L7 balancers handle HTTP request distribution. BalanceAI
handles AI-agent-specific routing concerns: KV-cache affinity, A2A protocol
health checks, agent-platform telemetry. The three bolded rows are the
defensible gap.

## Configuration

All knobs are env vars, defaults are production-friendly:

| Var                                  | Default                                              |
|--------------------------------------|------------------------------------------------------|
| `K8S_ENABLED`                        | `true` (mirrors registry.py)                         |
| `AGENTS_NAMESPACE`                   | `nasiko-agents` (K8s mode)                           |
| `AGENTS_NETWORK`                     | `agents-net` (Docker mode)                           |
| `BALANCER_REFRESH_INTERVAL_S`        | `5`                                                  |
| `BALANCER_HEALTH_INTERVAL_S`         | `5`                                                  |
| `BALANCER_HEALTH_TIMEOUT_S`          | `2`                                                  |
| `BALANCER_CB_FAILURE_THRESHOLD`      | `5`                                                  |
| `BALANCER_CB_COOLDOWN_INITIAL_S`     | `10`                                                 |
| `BALANCER_CB_COOLDOWN_MAX_S`         | `300`                                                |
| `BALANCER_SLOW_START_WINDOW_S`       | `30`                                                 |
| `BALANCER_TRACING`                   | `true`                                               |
| `PHOENIX_OTLP_ENDPOINT`              | `http://phoenix-observability:4318/v1/traces`        |

See [`RUNBOOK.md`](RUNBOOK.md) for failure-mode playbooks and SLOs.

## Running the quality gates

```bash
cd agent-gateway/router
./scripts/run_quality_gates.sh
```

Outputs: test results, coverage (if `pytest-cov` installed), Gini fairness
benchmark (RR < 0.05, P2C < 0.10 on 1000 picks across 5 replicas).
