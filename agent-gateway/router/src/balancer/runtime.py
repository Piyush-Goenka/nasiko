"""
Module-level singletons for the balancer.

The router process holds at most one LoadBalancer per agent_name, one
list of passive-observation callbacks (typically just the HealthChecker),
and one shared httpx.AsyncClient for the agent-call hot path.
Lookups are O(1) and lock-free.
"""

from typing import Callable, Optional

import httpx

from .load_balancer import LoadBalancer

_BALANCERS: dict[str, LoadBalancer] = {}
_PASSIVE_OBSERVERS: list[Callable] = []
_SHARED_HTTP_CLIENT: Optional[httpx.AsyncClient] = None


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


def setup_shared_http_client(
    timeout: httpx.Timeout | float,
    max_connections: int = 100,
    max_keepalive_connections: int = 20,
) -> httpx.AsyncClient:
    """
    Initialize the process-wide httpx.AsyncClient used by AgentClient on the
    LB hot path. Call once at FastAPI startup so the client lives inside the
    event loop. Without this, send_request opens a fresh client per call,
    costing ~one TCP handshake per request at 200 RPS.
    """
    global _SHARED_HTTP_CLIENT
    if _SHARED_HTTP_CLIENT is not None:
        return _SHARED_HTTP_CLIENT
    _SHARED_HTTP_CLIENT = httpx.AsyncClient(
        timeout=timeout,
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        ),
    )
    return _SHARED_HTTP_CLIENT


def get_shared_http_client() -> Optional[httpx.AsyncClient]:
    return _SHARED_HTTP_CLIENT


async def close_shared_http_client() -> None:
    """Call from FastAPI shutdown to release sockets cleanly."""
    global _SHARED_HTTP_CLIENT
    if _SHARED_HTTP_CLIENT is not None:
        await _SHARED_HTTP_CLIENT.aclose()
        _SHARED_HTTP_CLIENT = None


def reset_shared_http_client() -> None:
    """Test hook: forget the singleton without awaiting close."""
    global _SHARED_HTTP_CLIENT
    _SHARED_HTTP_CLIENT = None
