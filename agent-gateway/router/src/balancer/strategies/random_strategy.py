import secrets
from ..models import Replica


class Random:
    name = "random"

    def pick(self, candidates: list[Replica]) -> Replica:
        return secrets.choice(candidates)
