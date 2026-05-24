import time
import pytest
from router.src.balancer.circuit_breaker import CircuitBreaker, CircuitState
from router.src.balancer.load_balancer import LoadBalancer, NoHealthyReplica
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.slow_start import SlowStart
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
