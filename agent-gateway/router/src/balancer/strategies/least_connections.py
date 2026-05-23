from ..models import Replica
from ..slow_start import SlowStart


class LeastConnections:
    name = "least_connections"

    def __init__(self, slow_start: SlowStart):
        self._ss = slow_start

    def pick(self, candidates: list[Replica]) -> Replica:
        def score(r: Replica) -> float:
            w = self._ss.weight(r)
            return r.inflight / max(w, 0.01)

        return min(candidates, key=score)
