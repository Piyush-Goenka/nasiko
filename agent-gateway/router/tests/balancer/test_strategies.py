import time
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.slow_start import SlowStart
from router.src.balancer.strategies.round_robin import RoundRobin
from router.src.balancer.strategies.random_strategy import Random
from router.src.balancer.strategies.least_connections import LeastConnections
from router.src.balancer.strategies.p2c import P2C


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


def test_p2c_picks_lower_inflight_of_two_samples():
    ss = SlowStart(window_seconds=0)
    p2c = P2C(slow_start=ss)
    a, b = _r("a"), _r("b")
    a.inflight, b.inflight = 0, 10
    picks = [p2c.pick([a, b]).id for _ in range(50)]
    assert picks.count("a") == 50  # with only 2 candidates, P2C is deterministic on score


def test_p2c_single_candidate_returns_it():
    ss = SlowStart(window_seconds=0)
    p2c = P2C(slow_start=ss)
    a = _r("a")
    assert p2c.pick([a]).id == "a"


def test_p2c_distribution_over_many_picks():
    ss = SlowStart(window_seconds=0)
    p2c = P2C(slow_start=ss)
    cands = [_r(x) for x in ("a", "b", "c", "d", "e")]
    counts = {c.id: 0 for c in cands}
    for _ in range(5000):
        counts[p2c.pick(cands).id] += 1
    # max load should be << pure-random's expectation; loose bound for stability
    assert max(counts.values()) < 1500  # ~1000 expected, allow noise
