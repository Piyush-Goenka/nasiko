import itertools
from ..models import Replica


class RoundRobin:
    name = "round_robin"

    def __init__(self) -> None:
        self._counter = itertools.count()

    def pick(self, candidates: list[Replica]) -> Replica:
        i = next(self._counter) % len(candidates)
        return candidates[i]
