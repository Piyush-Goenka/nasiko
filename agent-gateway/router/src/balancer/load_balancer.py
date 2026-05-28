import inspect
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


def _strategy_accepts_routing_key(strategy: Strategy) -> bool:
    """
    Decide whether a strategy wants the routing_key kwarg.

    Two signals, in priority order:
      1. `affinity = True` class attribute (CHWBL declares this).
      2. `routing_key` appears in pick()'s signature.

    The signature check protects against the regression where a strategy
    accepts routing_key (via **kwargs or an explicit param) but forgets
    to declare `affinity`. We resolve this once per strategy swap and
    cache the result.
    """
    if getattr(strategy, "affinity", False):
        return True
    try:
        sig = inspect.signature(strategy.pick)
    except (TypeError, ValueError):
        return False
    params = sig.parameters
    if "routing_key" in params:
        return True
    for p in params.values():
        if p.kind is inspect.Parameter.VAR_KEYWORD:
            return True
    return False


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
        self._strategy_uses_key = _strategy_accepts_routing_key(strategy)
        self._strategy_weight_aware = getattr(strategy, "weight_aware", False)
        self._breakers = breakers
        self._slow_start = slow_start or SlowStart()
        self.hedging_enabled = hedging_enabled
        self.latency_tracker = latency_tracker

    @property
    def strategy_name(self) -> str:
        return self._strategy.name

    def set_strategy(self, name: str) -> None:
        self._strategy = _build(name, self._slow_start)
        self._strategy_uses_key = _strategy_accepts_routing_key(self._strategy)
        self._strategy_weight_aware = getattr(self._strategy, "weight_aware", False)

    def _strategy_pick(self, candidates: list[Replica],
                       routing_key: str | None) -> Replica:
        # Cached at construction (and re-cached on set_strategy) so the hot
        # path branches on a boolean rather than try/except TypeError, which
        # would silently swallow real TypeErrors raised from inside pick().
        if self._strategy_uses_key:
            return self._strategy.pick(candidates, routing_key=routing_key)
        return self._strategy.pick(candidates)

    def pick(self, routing_key: str | None = None) -> Replica:
        replica, _ = self.pick_with_stats(routing_key)
        return replica

    def pick_with_stats(self, routing_key: str | None = None) -> tuple[Replica, dict]:
        """
        Returns (replica, stats). The stats dict carries the numbers the OTel
        `lb.route` span needs (pool_size, healthy_count, candidates_considered,
        inflight_at_selection) so callers do not need to walk the registry a
        second time. Strategy decision latency is recorded by the caller, which
        owns the perf_counter.

        IMPORTANT: this method atomically increments `picked.inflight` before
        returning. Strategies that read inflight (LeastConnections, P2C) see a
        consistent view of the pool because no other coroutine can interleave
        between selection and increment (no awaits inside this function). The
        caller MUST eventually decrement exactly once; AgentClient does it in
        the finally clause of `_lb_send` / `_hedge_send`.
        """
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
        # Slow-start re-pick: makes "watch the ramp" demo visible for
        # strategies that don't consult SlowStart.weight() themselves
        # (round_robin, random, chwbl). A replica at weight=0.2 gets
        # re-picked with P=0.8. P2C and LeastConnections already
        # deweight cold replicas inside their scoring, so re-picking
        # on top of them would double-deweight; we skip the re-pick
        # there. Pass routing_key through so CHWBL keeps session
        # affinity during ramps.
        if not self._strategy_weight_aware and len(candidates) > 1:
            w = self._slow_start.weight(picked)
            if w < 1.0 and random.random() > w:
                rest = [c for c in candidates if c.container_name != picked.container_name]
                if rest:
                    picked = self._strategy_pick(rest, routing_key)
        stats = {
            "pool_size": len(replicas),
            "healthy_count": sum(
                1 for r in replicas if r.status is ReplicaStatus.SERVING
            ),
            "candidates_considered": len(candidates),
            "inflight_at_selection": picked.inflight,
        }
        # Reserve the slot before returning so a concurrent pick on the next
        # request sees this one as already in flight. Closes the herd race
        # in LeastConnections / P2C under burst load.
        picked.inflight += 1
        return picked, stats

    def pick_secondary(self, primary: Replica,
                       routing_key: str | None = None) -> Replica | None:
        """
        Pick a distinct, healthy replica for hedging against `primary`.
        Pre-increments inflight on the secondary (same contract as
        pick_with_stats). Returns None when no other candidate is healthy.
        """
        replicas = self._registry.replicas_for(self.agent_name)
        candidates = [
            r for r in replicas
            if r.status is ReplicaStatus.SERVING
            and r.container_name != primary.container_name
            and self._breakers.get(r.container_name, _NEUTRAL_BREAKER).can_pass()
        ]
        if not candidates:
            return None
        secondary = self._strategy_pick(candidates, routing_key)
        secondary.inflight += 1
        return secondary
