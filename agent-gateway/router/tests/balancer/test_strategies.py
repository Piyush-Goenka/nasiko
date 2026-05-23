import time
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.strategies.round_robin import RoundRobin


def _r(name):
    return Replica(
        id=name,
        agent_name="t",
        container_name=name,
        addr=f"http://{name}:5000",
        status=ReplicaStatus.SERVING,
        joined_at=time.monotonic(),
    )


def test_round_robin_cycles_through_candidates():
    rr = RoundRobin()
    cands = [_r("a"), _r("b"), _r("c")]
    seq = [rr.pick(cands).id for _ in range(6)]
    assert seq == ["a", "b", "c", "a", "b", "c"]


def test_round_robin_handles_single_replica():
    rr = RoundRobin()
    cands = [_r("a")]
    assert rr.pick(cands).id == "a"
    assert rr.pick(cands).id == "a"
