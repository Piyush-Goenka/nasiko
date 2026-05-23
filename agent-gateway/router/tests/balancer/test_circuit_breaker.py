import time
from router.src.balancer.circuit_breaker import CircuitBreaker, CircuitState


def test_starts_closed():
    cb = CircuitBreaker(failure_threshold=3, cooldown_initial=1.0, cooldown_max=10.0)
    assert cb.state is CircuitState.CLOSED
    assert cb.can_pass() is True


def test_trips_after_threshold_failures():
    cb = CircuitBreaker(failure_threshold=3, cooldown_initial=1.0, cooldown_max=10.0)
    for _ in range(3):
        cb.on_failure()
    assert cb.state is CircuitState.OPEN
    assert cb.can_pass() is False


def test_half_open_after_cooldown(monkeypatch):
    t = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: t[0])
    cb = CircuitBreaker(failure_threshold=1, cooldown_initial=5.0, cooldown_max=60.0)
    cb.on_failure()
    assert cb.state is CircuitState.OPEN
    t[0] = 5.5
    # First can_pass after cooldown moves us into HALF_OPEN and admits the probe
    assert cb.can_pass() is True
    assert cb.state is CircuitState.HALF_OPEN
    # Second can_pass in HALF_OPEN refuses (single probe only)
    assert cb.can_pass() is False


def test_half_open_success_closes(monkeypatch):
    t = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: t[0])
    cb = CircuitBreaker(failure_threshold=1, cooldown_initial=5.0, cooldown_max=60.0)
    cb.on_failure()
    t[0] = 5.5
    cb.can_pass()  # -> HALF_OPEN, probe admitted
    cb.on_success()
    assert cb.state is CircuitState.CLOSED


def test_half_open_failure_reopens_with_doubled_cooldown(monkeypatch):
    t = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: t[0])
    cb = CircuitBreaker(failure_threshold=1, cooldown_initial=5.0, cooldown_max=60.0)
    cb.on_failure()
    t[0] = 5.5
    cb.can_pass()  # -> HALF_OPEN
    cb.on_failure()
    assert cb.state is CircuitState.OPEN
    # cooldown doubled: should not pass until t = 5.5 + 10
    t[0] = 10.0
    assert cb.can_pass() is False
    t[0] = 15.6
    assert cb.can_pass() is True
