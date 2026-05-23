import time
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.slow_start import SlowStart
from router.src.balancer.strategies.round_robin import RoundRobin
from router.src.balancer.strategies.random_strategy import Random
from router.src.balancer.strategies.least_connections import LeastConnections


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


def test_random_picks_only_from_candidates():
    cands = [_r("a"), _r("b"), _r("c")]
    rand = Random()
    seen = {rand.pick(cands).id for _ in range(100)}
    assert seen.issubset({"a", "b", "c"})
    assert len(seen) > 1  # extremely unlikely to be otherwise


def test_least_connections_picks_lowest_inflight():
    ss = SlowStart(window_seconds=0)  # disable ramp for unit test
    lc = LeastConnections(slow_start=ss)
    a, b, c = _r("a"), _r("b"), _r("c")
    a.inflight, b.inflight, c.inflight = 5, 1, 3
    assert lc.pick([a, b, c]).id == "b"
