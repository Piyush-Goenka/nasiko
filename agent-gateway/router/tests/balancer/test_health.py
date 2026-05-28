import time
import asyncio
import pytest
from unittest.mock import AsyncMock
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.circuit_breaker import CircuitBreaker, CircuitState
from router.src.balancer.health import HealthChecker


def _r():
    return Replica(
        id="x", agent_name="t", container_name="x",
        addr="http://x:5000", status=ReplicaStatus.SERVING,
        joined_at=time.monotonic(),
    )


def test_passive_5xx_increments_counter_and_trips_breaker():
    r = _r()
    cb = CircuitBreaker(failure_threshold=5, cooldown_initial=1.0, cooldown_max=10.0)
    hc = HealthChecker(breakers={r.container_name: cb})
    for _ in range(5):
        hc.passive_observe(r, status_code=500, latency_ms=100.0)
    assert cb.state is CircuitState.OPEN
    assert r.consecutive_5xx == 5


def test_passive_2xx_resets_counter():
    r = _r()
    cb = CircuitBreaker(failure_threshold=5, cooldown_initial=1.0, cooldown_max=10.0)
    hc = HealthChecker(breakers={r.container_name: cb})
    for _ in range(3):
        hc.passive_observe(r, status_code=500, latency_ms=100.0)
    hc.passive_observe(r, status_code=200, latency_ms=50.0)
    assert r.consecutive_5xx == 0
    assert cb.state is CircuitState.CLOSED


def test_passive_4xx_does_not_heal_half_open_breaker():
    """Regression for C3: a 4xx is a client error, not evidence the replica
    is healthy. The breaker must stay OPEN on a 4xx received in HALF_OPEN
    state; otherwise an auth-failure storm silently keeps a degraded backend
    in the pool."""
    r = _r()
    cb = CircuitBreaker(failure_threshold=2, cooldown_initial=0.01, cooldown_max=1.0)
    hc = HealthChecker(breakers={r.container_name: cb})
    # Trip the breaker
    for _ in range(2):
        hc.passive_observe(r, status_code=500, latency_ms=10.0)
    assert cb.state is CircuitState.OPEN
    # Let cooldown elapse so the next can_pass() promotes to HALF_OPEN
    time.sleep(0.02)
    assert cb.can_pass() is True
    assert cb.state is CircuitState.HALF_OPEN
    # A 4xx now must NOT close the breaker
    hc.passive_observe(r, status_code=401, latency_ms=5.0)
    assert cb.state is CircuitState.HALF_OPEN, (
        "4xx must not be treated as evidence the replica is healthy"
    )
    # A real 2xx, however, does close it
    hc.passive_observe(r, status_code=200, latency_ms=5.0)
    assert cb.state is CircuitState.CLOSED


def test_passive_4xx_resets_consecutive_5xx_counter():
    """A 4xx is not a fault but does interrupt a 5xx run: the counter
    should reset so a later legitimate 5xx burst measures only that burst."""
    r = _r()
    cb = CircuitBreaker(failure_threshold=5, cooldown_initial=1.0, cooldown_max=10.0)
    hc = HealthChecker(breakers={r.container_name: cb})
    for _ in range(3):
        hc.passive_observe(r, status_code=500, latency_ms=100.0)
    assert r.consecutive_5xx == 3
    hc.passive_observe(r, status_code=404, latency_ms=5.0)
    assert r.consecutive_5xx == 0
    # Breaker should NOT have been "healed" by the 4xx
    assert cb._consecutive_failures == 3


def test_passive_observe_updates_ewma_latency():
    r = _r()
    cb = CircuitBreaker()
    hc = HealthChecker(breakers={r.container_name: cb})
    hc.passive_observe(r, status_code=200, latency_ms=100.0)
    first = r.ewma_latency_ms
    # Force time delta so alpha > 0 and EWMA moves toward newer sample
    time.sleep(0.01)
    hc.passive_observe(r, status_code=200, latency_ms=200.0)
    assert r.ewma_latency_ms > first  # moved toward 200


@pytest.mark.asyncio
async def test_active_probe_promotes_discovered_to_serving():
    r = _r()
    r.status = ReplicaStatus.DISCOVERED
    cb = CircuitBreaker()

    class _Resp:
        status_code = 200

    fake_get = AsyncMock(return_value=_Resp())
    hc = HealthChecker(
        breakers={r.container_name: cb},
        http_get=fake_get,
        interval=0.01,
    )
    await hc.run_probe(r, stop_after=0.05)
    assert r.status is ReplicaStatus.SERVING


@pytest.mark.asyncio
async def test_active_probe_failures_trip_breaker():
    r = _r()
    cb = CircuitBreaker(failure_threshold=5)

    async def boom(*a, **k):
        raise RuntimeError("down")

    hc = HealthChecker(
        breakers={r.container_name: cb},
        http_get=boom,
        interval=0.01,
        timeout=0.01,  # short TCP-fallback timeout so the test loop iterates
        active_failure_threshold=2,
    )
    await hc.run_probe(r, stop_after=0.2)
    assert r.consecutive_health_failures >= 2
    # Two consecutive active failures explicitly trip the breaker
    assert cb.state in (CircuitState.OPEN, CircuitState.HALF_OPEN)
