import time
import hashlib
from unittest.mock import patch
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


def test_chwbl_ring_is_cached_across_pick_calls():
    """Regression for T6: the consistent-hash ring must rebuild only when
    membership changes. Without caching, every pick does N*virtual_nodes
    SHA-1 hashes (e.g., 384 hashes/pick for a 3-replica pool with vnodes=128)."""
    cands = [_r("a"), _r("b"), _r("c")]
    ch = CHWBL(c=1.25, virtual_nodes=128)
    # Warm the cache with one pick.
    ch.pick(cands, routing_key="warmup")
    real_sha1 = hashlib.sha1
    call_count = {"n": 0}

    def _counting_sha1(*a, **kw):
        call_count["n"] += 1
        return real_sha1(*a, **kw)

    with patch("router.src.balancer.strategies.chwbl.hashlib.sha1", new=_counting_sha1):
        for i in range(100):
            ch.pick(cands, routing_key=f"key-{i}")
        # Ring is cached -> only the key hash per call. Without caching this
        # would be 100 * (384 + 1) = 38500. With caching: ~100.
        assert call_count["n"] <= 110, (
            f"Ring should be cached; expected ~100 SHA-1 calls, got {call_count['n']}"
        )


def test_chwbl_ring_rebuilds_when_membership_changes():
    """Ring cache must invalidate when a replica joins or leaves."""
    a, b, c = _r("a"), _r("b"), _r("c")
    ch = CHWBL()
    ch.pick([a, b, c], routing_key="x")
    cache1 = (ch._cache_key, list(ch._cache_ring))
    # Same membership -> same cache key, ring unchanged.
    ch.pick([a, b, c], routing_key="y")
    assert (ch._cache_key, list(ch._cache_ring)) == cache1
    # Drop c -> cache key differs, ring rebuilt.
    ch.pick([a, b], routing_key="z")
    assert ch._cache_key != cache1[0]


def test_chwbl_cache_refreshes_replica_references():
    """Owners dict tracks Replica objects. When the registry rebuilds
    Replica instances (same container_name, new object), the cache must
    serve the fresh object so callers see current inflight/status."""
    ch = CHWBL()
    a1 = _r("a")
    b1 = _r("b")
    ch.pick([a1, b1], routing_key="x")
    # Registry rebuilds the Replica objects with fresh inflight counters.
    a2 = _r("a")
    b2 = _r("b")
    a2.inflight = 99
    picked = ch.pick([a2, b2], routing_key="x")
    # Same routing_key, same membership -> same container_name; but the
    # returned object must be the fresh one so its inflight is observable.
    assert picked.container_name == picked.container_name  # tautology to anchor below
    assert picked.inflight == (99 if picked.container_name == "a" else 0)
