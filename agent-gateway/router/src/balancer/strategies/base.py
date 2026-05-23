from typing import Protocol
from ..models import Replica


class Strategy(Protocol):
    name: str

    def pick(self, candidates: list[Replica]) -> Replica: ...
