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

    def __init__(self, c: float = 1.25, virtual_nodes: int = 128):
        self._c = c
        self._vnodes = virtual_nodes

    def _ring(self, candidates: list[Replica]) -> tuple[list[int], dict[int, Replica]]:
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
