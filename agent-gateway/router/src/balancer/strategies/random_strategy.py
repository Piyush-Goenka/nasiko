import random
from ..models import Replica


class Random:
    """Pure-random strategy.

    Uses `random.choice` (Mersenne Twister) rather than `secrets.choice`.
    Load-balancer fairness only needs uniform distribution, not cryptographic
    unpredictability, and `secrets` adds non-trivial overhead per pick.
    Aligns with the slow-start re-pick in load_balancer.py, which also uses
    the `random` module.
    """

    name = "random"

    def pick(self, candidates: list[Replica]) -> Replica:
        return random.choice(candidates)
