import time
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.strategies.chwbl import CHWBL


def _r(name):
    return Replica(
        id=name, agent_name="t", container_name=name,
        addr=f"http://{name}:5000", status=ReplicaStatus.SERVING,
        joined_at=time.monotonic(),
    )


def test_chwbl_same_key_same_replica_when_under_bound():
    cands = [_r("a"), _r("b"), _r("c")]
    ch = CHWBL(c=1.25, virtual_nodes=128)
    picks = {ch.pick(cands, routing_key="session-42").id for _ in range(50)}
    assert len(picks) == 1  # same key -> same replica


def test_chwbl_different_keys_spread_across_replicas():
    cands = [_r("a"), _r("b"), _r("c")]
    ch = CHWBL(c=1.25, virtual_nodes=128)
    picks = {ch.pick(cands, routing_key=f"session-{i}").id for i in range(300)}
    assert len(picks) == 3  # all replicas eventually used


def test_chwbl_overflow_spills_when_over_bound():
    a, b, c = _r("a"), _r("b"), _r("c")
    a.inflight = 100
    b.inflight = 0
    c.inflight = 0
    ch = CHWBL(c=1.25, virtual_nodes=128)
    # cap = 1.25 * (100/3) ≈ 42 — "a" is over cap and must spill at least once
    spillover_seen = False
    for i in range(50):
        picked = ch.pick([a, b, c], routing_key=f"hot-key-{i}")
        if picked.id != "a":
            spillover_seen = True
            break
    assert spillover_seen


def test_chwbl_no_key_falls_back_to_least_connections():
    a, b = _r("a"), _r("b")
    a.inflight, b.inflight = 10, 1
    ch = CHWBL()
    assert ch.pick([a, b], routing_key=None).id == "b"
