import time
import pytest
from router.src.balancer.circuit_breaker import CircuitBreaker, CircuitState
from router.src.balancer.load_balancer import LoadBalancer, NoHealthyReplica
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.slow_start import SlowStart
from router.src.balancer.strategies.chwbl import CHWBL
from router.src.balancer.strategies.round_robin import RoundRobin


def _r(name, status=ReplicaStatus.SERVING):
    return Replica(
        id=name, agent_name="t", container_name=name,
        addr=f"http://{name}:5000", status=status,
        joined_at=time.monotonic() - 1000,  # past slow-start window
    )


class _FakeReg:
    def __init__(self, replicas):
        self.replicas = replicas

    def replicas_for(self, name):
        return self.replicas


def test_pick_returns_serving_replica():
    replicas = [_r("a"), _r("b"), _r("c")]
    cbs = {r.container_name: CircuitBreaker() for r in replicas}
    lb = LoadBalancer(
        agent_name="t", registry=_FakeReg(replicas),
        strategy=RoundRobin(), breakers=cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    assert lb.pick().container_name in {"a", "b", "c"}


def test_pick_excludes_open_circuit_replicas():
    a, b = _r("a"), _r("b")
    cbs = {
        "a": CircuitBreaker(failure_threshold=1, cooldown_initial=999),
        "b": CircuitBreaker(),
    }
    cbs["a"].on_failure()  # opens
    assert cbs["a"].state is CircuitState.OPEN
    lb = LoadBalancer(
        agent_name="t", registry=_FakeReg([a, b]),
        strategy=RoundRobin(), breakers=cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    for _ in range(10):
        assert lb.pick().container_name == "b"


def test_pick_excludes_non_serving_status():
    a = _r("a", status=ReplicaStatus.SERVING)
    b = _r("b", status=ReplicaStatus.DISCOVERED)
    cbs = {"a": CircuitBreaker(), "b": CircuitBreaker()}
    lb = LoadBalancer(
        agent_name="t", registry=_FakeReg([a, b]),
        strategy=RoundRobin(), breakers=cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    assert lb.pick().container_name == "a"


def test_pick_raises_when_no_healthy_replica():
    a = _r("a")
    cbs = {"a": CircuitBreaker(failure_threshold=1)}
    cbs["a"].on_failure()  # open
    lb = LoadBalancer(
        agent_name="t", registry=_FakeReg([a]),
        strategy=RoundRobin(), breakers=cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    with pytest.raises(NoHealthyReplica):
        lb.pick()


def test_routing_key_reaches_strategy_on_both_pick_and_repick():
    """Regression for T2: slow-start re-pick previously called strategy.pick
    without routing_key, silently degrading CHWBL to least-connections. This
    test uses a spy strategy to assert routing_key flows through both the
    initial pick and the slow-start re-pick branches."""
    seen_keys: list[str | None] = []

    class _SpyStrategy:
        name = "spy"

        def pick(self, candidates, routing_key=None):
            seen_keys.append(routing_key)
            return candidates[0]

    # Mid-ramp so re-pick fires deterministically (weight ~0.17 < 1).
    now = time.monotonic()
    replicas = [
        Replica(id=n, agent_name="t", container_name=n,
                addr=f"http://{n}:5000", status=ReplicaStatus.SERVING,
                joined_at=now - 5)
        for n in ("a", "b")
    ]
    cbs = {r.container_name: CircuitBreaker() for r in replicas}
    lb = LoadBalancer(
        agent_name="t", registry=_FakeReg(replicas),
        strategy=_SpyStrategy(), breakers=cbs,
        slow_start=SlowStart(window_seconds=30.0),
    )
    for _ in range(50):
        lb.pick(routing_key="session-stable")
    # Some calls produce 1 pick (weight check skipped), some produce 2 (re-pick fired).
    # Every recorded call must carry the routing_key.
    assert seen_keys, "spy strategy was never invoked"
    assert all(k == "session-stable" for k in seen_keys), (
        f"routing_key dropped on some pick path; saw {set(seen_keys)}"
    )
    # And the re-pick branch was actually exercised (more than one strategy
    # call per pick on at least some iterations).
    assert len(seen_keys) > 50, (
        f"slow-start re-pick never fired (recorded {len(seen_keys)} calls "
        f"for 50 picks); test premise is wrong"
    )


def test_set_strategy_hot_swaps():
    replicas = [_r("a"), _r("b")]
    cbs = {r.container_name: CircuitBreaker() for r in replicas}
    lb = LoadBalancer(
        agent_name="t", registry=_FakeReg(replicas),
        strategy=RoundRobin(), breakers=cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    lb.set_strategy("random")
    assert lb.strategy_name == "random"
    lb.set_strategy("round_robin")
    assert lb.strategy_name == "round_robin"
