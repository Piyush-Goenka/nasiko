import math
import time
from collections import deque
from threading import Lock
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

REGISTRY = CollectorRegistry()

requests_total = Counter(
    "lb_requests_total", "Total LB-routed requests",
    ["pool", "instance_id", "status_class"], registry=REGISTRY,
)
request_duration_seconds = Histogram(
    "lb_request_duration_seconds", "End-to-end request duration",
    ["pool", "instance_id"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
    registry=REGISTRY,
)
inflight = Gauge(
    "lb_inflight", "Currently in-flight requests",
    ["pool", "instance_id"], registry=REGISTRY,
)
pool_size = Gauge(
    "lb_pool_size", "Replicas by status",
    ["pool", "status"], registry=REGISTRY,
)
circuit_state = Gauge(
    "lb_circuit_state", "Circuit breaker state (0 closed, 1 half_open, 2 open)",
    ["pool", "instance_id"], registry=REGISTRY,
)
fairness_gini = Gauge(
    "lb_fairness_gini", "Gini coefficient of request distribution",
    ["pool"], registry=REGISTRY,
)
active_strategy = Gauge(
    "lb_strategy", "Active strategy (1 = active)",
    ["pool", "strategy"], registry=REGISTRY,
)
route_decision_us = Histogram(
    "lb_route_decision_microseconds", "Strategy decision latency",
    ["pool", "strategy"],
    buckets=(1, 10, 50, 100, 500, 1000, 5000),
    registry=REGISTRY,
)


def publish_pool_gauges(pool: str, replicas, breakers, active_strategy_name: str) -> None:
    """
    Refresh the per-pool gauges that the dashboard and Phoenix scrape.

    These three gauges are pull-driven (set whenever someone looks at
    /balancer/pools) rather than emitted at every transition. That keeps
    the hot path lock-free and saves a write per request while still
    giving the dashboard fresh numbers at its poll cadence.

    - lb_pool_size{pool, status}: replicas in each lifecycle state
    - lb_circuit_state{pool, instance_id}: 0 closed, 1 half_open, 2 open
    - lb_strategy{pool, strategy}: 1 for the active strategy, 0 for the rest
    """
    from .circuit_breaker import CircuitState

    counts: dict[str, int] = {}
    for r in replicas:
        s = r.status.value if hasattr(r.status, "value") else str(r.status)
        counts[s] = counts.get(s, 0) + 1
        cb = breakers.get(r.container_name)
        if cb is None:
            state_val = 0
        elif cb.state is CircuitState.HALF_OPEN:
            state_val = 1
        elif cb.state is CircuitState.OPEN:
            state_val = 2
        else:
            state_val = 0
        circuit_state.labels(pool, r.container_name).set(state_val)
    # Always publish all known statuses (zero out drained ones) so panels
    # don't keep a stale gauge alive after a status transitions away.
    for s in ("discovered", "ready", "serving", "draining", "ejected", "terminated"):
        pool_size.labels(pool, s).set(counts.get(s, 0))

    for name in _STRATEGY_NAMES:
        active_strategy.labels(pool, name).set(1 if name == active_strategy_name else 0)


_STRATEGY_NAMES = ("round_robin", "random", "least_connections", "p2c", "chwbl")


def status_class(code: int) -> str:
    if 200 <= code < 300:
        return "2xx"
    if 300 <= code < 400:
        return "3xx"
    if 400 <= code < 500:
        return "4xx"
    return "5xx"


def gini(values: list[float]) -> float:
    """
    Gini coefficient of request distribution across replicas.

    Returns a value in [0, 1]:
      0 = perfect equality (every replica got the same)
      1 = total concentration (one replica got everything)

    Closed form: after sorting ascending, G = Sum((2i - N - 1) * x_i) / (N * Sum(x))
    for i in [1..N]. O(N log N), matches the naive O(N^2) double-sum to floating
    point tolerance. Worth it at N >= ~50 replicas.
    """
    if not values:
        return 0.0
    n = len(values)
    total = sum(values)
    if total == 0:
        return 0.0
    sorted_vals = sorted(values)
    cumulative = 0.0
    for i, v in enumerate(sorted_vals, start=1):
        cumulative += (2 * i - n - 1) * v
    return cumulative / (n * total)


class RequestCounter:
    """
    Per-replica rolling 60-second request count, suitable as the x_i input to
    the Gini fairness metric.

    Why this is needed: the dashboard's "hero" metric is fairness across
    replicas. Reading `r.inflight` (instantaneous in-flight count) collapses to
    zero whenever the cluster is between bursts, which hides the very
    imbalance the operator is trying to spot. A 60-second window keeps
    historical pressure visible without growing unbounded.

    Thread-safety: dispatch happens on asyncio (single thread), but Prometheus
    scrape happens on a worker thread, so reads must be locked.
    """

    def __init__(self, window_seconds: float = 60.0):
        self._window = window_seconds
        self._events: dict[tuple[str, str], deque[float]] = {}
        self._lock = Lock()

    def record(self, pool: str, instance_id: str, now: float | None = None) -> None:
        ts = time.monotonic() if now is None else now
        key = (pool, instance_id)
        with self._lock:
            q = self._events.get(key)
            if q is None:
                q = deque()
                self._events[key] = q
            q.append(ts)
            cutoff = ts - self._window
            while q and q[0] < cutoff:
                q.popleft()

    def counts_for(self, pool: str, instance_ids: list[str],
                   now: float | None = None) -> list[int]:
        ts = time.monotonic() if now is None else now
        cutoff = ts - self._window
        out: list[int] = []
        with self._lock:
            for inst in instance_ids:
                q = self._events.get((pool, inst))
                if q is None:
                    out.append(0)
                    continue
                while q and q[0] < cutoff:
                    q.popleft()
                out.append(len(q))
        return out

    def reset(self) -> None:
        with self._lock:
            self._events.clear()


# Module-level singleton: agent_client records on every routed request;
# api.py reads counts when computing the dashboard Gini gauge.
request_counter = RequestCounter(window_seconds=60.0)


def coefficient_of_variation(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(var) / mean
