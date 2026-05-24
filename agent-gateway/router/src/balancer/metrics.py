import math
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

    O(N^2) by definition; fine for pool sizes <= 100.
    """
    if not values:
        return 0.0
    n = len(values)
    s = sum(values)
    if s == 0:
        return 0.0
    diffs = sum(abs(a - b) for a in values for b in values)
    return diffs / (2 * n * n * (s / n))


def coefficient_of_variation(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(var) / mean
