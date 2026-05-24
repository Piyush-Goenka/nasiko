"""
Module-level singletons for the balancer.

The router process holds at most one LoadBalancer per agent_name and one
list of passive-observation callbacks (typically just the HealthChecker).
Lookups are O(1) and lock-free on the hot path.
"""

from typing import Callable
from .load_balancer import LoadBalancer

_BALANCERS: dict[str, LoadBalancer] = {}
_PASSIVE_OBSERVERS: list[Callable] = []


def set_balancer_for(agent_name: str, lb: LoadBalancer) -> None:
    _BALANCERS[agent_name] = lb


def get_balancer_for(agent_name: str) -> LoadBalancer | None:
    return _BALANCERS.get(agent_name)


def all_balancers() -> dict[str, LoadBalancer]:
    return dict(_BALANCERS)


def clear_balancers() -> None:
    _BALANCERS.clear()


def register_passive_observer(fn: Callable) -> None:
    _PASSIVE_OBSERVERS.append(fn)


def passive_observe_hook(replica, status_code: int, latency_ms: float) -> None:
    for fn in _PASSIVE_OBSERVERS:
        try:
            fn(replica, status_code, latency_ms)
        except Exception:
            pass


def clear_passive_observers() -> None:
    _PASSIVE_OBSERVERS.clear()
