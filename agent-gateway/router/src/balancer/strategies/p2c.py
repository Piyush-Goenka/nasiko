import random
from ..models import Replica
from ..slow_start import SlowStart


class P2C:
    """
    Power of Two Choices (Mitzenmacher 2001).

    Samples two random replicas, picks the less-loaded one.
    Max load is O(log log N / log 2), doubly-exponentially better than pure random.
    Avoids the least-connections cold-start herd because each router sees different
    random pairs, so 50 routers don't synchronize on the same "winner."
    """

    name = "p2c"
    # P2C consults SlowStart.weight() when scoring candidates, so the
    # LoadBalancer's outer slow-start re-pick must NOT fire on top of it.
    # Otherwise cold replicas get deweighted twice and effectively never
    # serve traffic during their ramp.
    weight_aware = True

    def __init__(self, slow_start: SlowStart):
        self._ss = slow_start

    def pick(self, candidates: list[Replica]) -> Replica:
        if len(candidates) == 1:
            return candidates[0]
        a, b = random.sample(candidates, 2)
        sa = a.inflight / max(self._ss.weight(a), 0.01)
        sb = b.inflight / max(self._ss.weight(b), 0.01)
        return a if sa <= sb else b
