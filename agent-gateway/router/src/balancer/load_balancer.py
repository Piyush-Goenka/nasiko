import random
from .circuit_breaker import CircuitBreaker
from .models import Replica, ReplicaStatus
from .slow_start import SlowStart
from .strategies.base import Strategy
from .strategies.least_connections import LeastConnections
from .strategies.p2c import P2C
from .strategies.random_strategy import Random
from .strategies.round_robin import RoundRobin


class NoHealthyReplica(RuntimeError):
    pass


def _build(name: str, slow_start: SlowStart) -> Strategy:
    if name == "round_robin":
        return RoundRobin()
    if name == "random":
        return Random()
    if name == "least_connections":
        return LeastConnections(slow_start)
    if name == "p2c":
        return P2C(slow_start)
    raise ValueError(f"unknown strategy: {name}")


class LoadBalancer:
    """
    Per-agent load balancer. Owns:
      - A reference to the shared InstanceRegistry (filters by agent_name).
      - A Strategy (mutable; swappable at runtime via set_strategy).
      - A reference to the shared CircuitBreaker registry.
      - A SlowStart policy applied to all strategies via probabilistic re-pick.
    """

    def __init__(self, agent_name: str, registry, strategy: Strategy,
                 breakers: dict[str, CircuitBreaker],
                 slow_start: SlowStart | None = None):
        self.agent_name = agent_name
        self._registry = registry
        self._strategy = strategy
        self._breakers = breakers
        self._slow_start = slow_start or SlowStart()

    @property
    def strategy_name(self) -> str:
        return self._strategy.name

    def set_strategy(self, name: str) -> None:
        self._strategy = _build(name, self._slow_start)

    def pick(self) -> Replica:
        replicas = self._registry.replicas_for(self.agent_name)
        candidates = [
            r for r in replicas
            if r.status is ReplicaStatus.SERVING
            and self._breakers.get(r.container_name, CircuitBreaker()).can_pass()
        ]
        if not candidates:
            raise NoHealthyReplica(f"no healthy replicas for {self.agent_name}")
        picked = self._strategy.pick(candidates)
        # Slow-start re-pick: makes "watch the ramp" demo visible regardless of
        # active strategy. A replica at weight=0.2 gets re-picked with P=0.8.
        if len(candidates) > 1:
            w = self._slow_start.weight(picked)
            if w < 1.0 and random.random() > w:
                rest = [c for c in candidates if c.container_name != picked.container_name]
                if rest:
                    picked = self._strategy.pick(rest)
        return picked
