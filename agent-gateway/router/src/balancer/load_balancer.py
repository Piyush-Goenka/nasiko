import random
from .circuit_breaker import CircuitBreaker
from .models import Replica, ReplicaStatus
from .slow_start import SlowStart
from .strategies.base import Strategy
from .strategies.chwbl import CHWBL
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
    if name == "chwbl":
        return CHWBL(c=1.25, virtual_nodes=128)
    raise ValueError(f"unknown strategy: {name}")


_NEUTRAL_BREAKER = CircuitBreaker()


class LoadBalancer:
    """
    Per-agent load balancer. Owns:
      - A reference to the shared InstanceRegistry (filters by agent_name).
      - A Strategy (mutable; swappable at runtime via set_strategy).
      - A reference to the shared CircuitBreaker registry.
      - A SlowStart policy applied to all strategies via probabilistic re-pick.

    Concurrency: safe under CPython single-thread + asyncio (the only deployment
    today). set_strategy is a plain attribute swap; in-flight picks complete
    against the previous strategy. Not safe under preemptive threads; add a
    threading.Lock around set_strategy if that ever changes.
    """

    def __init__(self, agent_name: str, registry, strategy: Strategy,
                 breakers: dict[str, CircuitBreaker],
                 slow_start: SlowStart | None = None,
                 hedging_enabled: bool = False,
                 latency_tracker=None):
        self.agent_name = agent_name
        self._registry = registry
        self._strategy = strategy
        self._breakers = breakers
        self._slow_start = slow_start or SlowStart()
        self.hedging_enabled = hedging_enabled
        self.latency_tracker = latency_tracker

    @property
    def strategy_name(self) -> str:
        return self._strategy.name

    def set_strategy(self, name: str) -> None:
        self._strategy = _build(name, self._slow_start)

    def _strategy_pick(self, candidates: list[Replica],
                       routing_key: str | None) -> Replica:
        try:
            return self._strategy.pick(candidates, routing_key=routing_key)
        except TypeError:
            return self._strategy.pick(candidates)

    def pick(self, routing_key: str | None = None) -> Replica:
        replicas = self._registry.replicas_for(self.agent_name)
        candidates: list[Replica] = []
        for r in replicas:
            if r.status is not ReplicaStatus.SERVING:
                continue
            cb = self._breakers.get(r.container_name)
            if cb is None:
                # Unregistered replica: treat as closed-circuit for one request.
                # Avoids allocating a CircuitBreaker per replica per pick.
                if _NEUTRAL_BREAKER.can_pass():
                    candidates.append(r)
            elif cb.can_pass():
                candidates.append(r)
        if not candidates:
            raise NoHealthyReplica(f"no healthy replicas for {self.agent_name}")
        picked = self._strategy_pick(candidates, routing_key)
        # Slow-start re-pick: makes "watch the ramp" demo visible regardless of
        # active strategy. A replica at weight=0.2 gets re-picked with P=0.8.
        # Pass routing_key through so CHWBL keeps session affinity during ramps.
        if len(candidates) > 1:
            w = self._slow_start.weight(picked)
            if w < 1.0 and random.random() > w:
                rest = [c for c in candidates if c.container_name != picked.container_name]
                if rest:
                    picked = self._strategy_pick(rest, routing_key)
        return picked

    def pick_two(self, routing_key: str | None = None) -> tuple[Replica, Replica | None]:
        """Pick a primary and (when possible) a distinct secondary for hedging."""
        primary = self.pick(routing_key=routing_key)
        replicas = self._registry.replicas_for(self.agent_name)
        candidates = [
            r for r in replicas
            if r.status is ReplicaStatus.SERVING
            and r.container_name != primary.container_name
            and self._breakers.get(r.container_name, _NEUTRAL_BREAKER).can_pass()
        ]
        if not candidates:
            return primary, None
        secondary = self._strategy_pick(candidates, routing_key)
        return primary, secondary
