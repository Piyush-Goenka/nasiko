from ..models import Replica
from ..slow_start import SlowStart


class LeastConnections:
    name = "least_connections"
    # Score already divides by SlowStart.weight(); the outer re-pick in
    # LoadBalancer would double-deweight cold replicas, biasing traffic away
    # from them more aggressively than the slow-start window intends.
    weight_aware = True

    def __init__(self, slow_start: SlowStart):
        self._ss = slow_start

    def pick(self, candidates: list[Replica]) -> Replica:
        def score(r: Replica) -> float:
            w = self._ss.weight(r)
            return r.inflight / max(w, 0.01)

        return min(candidates, key=score)
