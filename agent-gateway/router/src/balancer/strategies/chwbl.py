"""
Consistent Hashing with Bounded Loads (Mirrokni/Thorup/Zadimoghaddam, Google 2016).

Routes by routing_key (e.g. session_id) for KV-cache locality on LLM agents;
spills to the next ring node when the owner's load exceeds c * mean_load.

vLLM Router and SGLang use the same pattern for prefix-cache affinity.
"""

import hashlib
from bisect import bisect_right
from ..models import Replica


class CHWBL:
    name = "chwbl"
    # Affinity-bearing strategies want routing_key on every call, including
    # the slow-start re-pick path in LoadBalancer.pick.
    affinity = True

    def __init__(self, c: float = 1.25, virtual_nodes: int = 128):
        self._c = c
        self._vnodes = virtual_nodes
        # Ring cache: keyed by the tuple of container names in the candidate
        # set. Rebuilt only when membership changes (scale, ejection,
        # recovery). At 200 RPS over a 3-replica pool this cuts ~77k SHA-1
        # hashes/sec from the hot path.
        self._cache_key: tuple[str, ...] | None = None
        self._cache_ring: list[int] = []
        self._cache_owners: dict[int, Replica] = {}

    def _ring(self, candidates: list[Replica]) -> tuple[list[int], dict[int, Replica]]:
        key = tuple(sorted(r.container_name for r in candidates))
        if key == self._cache_key:
            # Owners are keyed by hash but Replica objects in the cache may be
            # stale if the registry rebuilt them. Refresh references in place.
            current = {r.container_name: r for r in candidates}
            owners = {h: current[r.container_name] for h, r in self._cache_owners.items()}
            return self._cache_ring, owners
        ring: list[int] = []
        owners: dict[int, Replica] = {}
        for r in candidates:
            for v in range(self._vnodes):
                h = int(hashlib.sha1(
                    f"{r.container_name}#{v}".encode()
                ).hexdigest()[:16], 16)
                ring.append(h)
                owners[h] = r
        ring.sort()
        self._cache_key = key
        self._cache_ring = ring
        self._cache_owners = owners
        return ring, owners

    def pick(self, candidates: list[Replica], routing_key: str | None = None) -> Replica:
        if not routing_key or len(candidates) == 1:
            # No key -> degrade to least-connections fallback
            return min(candidates, key=lambda r: r.inflight)
        ring, owners = self._ring(candidates)
        total_load = sum(r.inflight for r in candidates)
        n = len(candidates)
        cap = max(1, int((self._c * total_load) / n) + 1)
        key_hash = int(hashlib.sha1(routing_key.encode()).hexdigest()[:16], 16)
        start = bisect_right(ring, key_hash) % len(ring)
        for i in range(len(ring)):
            cand = owners[ring[(start + i) % len(ring)]]
            if cand.inflight < cap:
                return cand
        # All over cap (shouldn't happen with c >= 1.0)
        return min(candidates, key=lambda r: r.inflight)
