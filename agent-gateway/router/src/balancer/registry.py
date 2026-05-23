import asyncio
import re
import time
from typing import Callable
from .models import Replica, ReplicaStatus

_AGENT_NAME_RE = re.compile(r"^(?:agent-|a2a-)?([a-z0-9_-]+?)(?:-\d+)?$")
_INFRA_DENYLIST = {
    "kong-gateway", "kong-database", "redis", "mongodb",
    "phoenix-observability", "nasiko-router", "nasiko-web",
    "nasiko-backend", "nasiko-auth-service", "chat-history-service",
    "kong-service-registry", "kong-migrations",
    "nasiko-superuser-init", "nasiko-redis-listener",
}
# Nasiko reference agents use "a2a-" prefix (e.g. a2a-translator).
# Legacy "agent-" prefix kept for compat.
_AGENT_NAME_PREFIXES = ("agent-", "a2a-")


class InstanceRegistry:
    """
    Maintains the live pool of agent replicas discovered via Docker SDK.

    Replicas walk: DISCOVERED -> READY -> SERVING -> DRAINING -> TERMINATED.
    A periodic refresh detects new containers and marks missing ones terminated.
    Subscribers are notified on add/remove for downstream wiring (health probes,
    load-balancer pool updates, dashboard events).
    """

    def __init__(self, docker_client, network_name: str = "agents-net",
                 refresh_interval: float = 5.0):
        self._docker = docker_client
        self._network = network_name
        self._interval = refresh_interval
        self._replicas: dict[str, Replica] = {}  # keyed by container_name
        self._terminated: list[Replica] = []
        self._subscribers: list[Callable[[str, Replica], None]] = []
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()

    async def _run(self) -> None:
        while True:
            try:
                self.refresh_sync()
            except Exception:
                pass
            await asyncio.sleep(self._interval)

    def refresh_sync(self) -> None:
        network = self._docker.networks.get(self._network)
        seen: set[str] = set()
        for c in network.containers:
            if c.name in _INFRA_DENYLIST or not c.name.startswith(_AGENT_NAME_PREFIXES):
                continue
            seen.add(c.name)
            agent_name = self._agent_name(c)
            if c.name not in self._replicas:
                r = Replica(
                    id=c.short_id,
                    agent_name=agent_name,
                    container_name=c.name,
                    addr=f"http://{c.name}:5000",
                    status=ReplicaStatus.DISCOVERED,
                    joined_at=time.monotonic(),
                )
                self._replicas[c.name] = r
                self._notify("added", r)
        # mark missing as terminated
        for name in list(self._replicas):
            if name not in seen:
                r = self._replicas.pop(name)
                r.status = ReplicaStatus.TERMINATED
                self._terminated.append(r)
                self._notify("removed", r)

    def _agent_name(self, container) -> str:
        label = (container.labels or {}).get("com.nasiko.agent")
        if label:
            return label
        m = _AGENT_NAME_RE.match(container.name)
        return m.group(1) if m else container.name

    def replicas_for(self, agent_name: str) -> list[Replica]:
        return [r for r in self._replicas.values() if r.agent_name == agent_name]

    def all_replicas(self) -> list[Replica]:
        return list(self._replicas.values())

    def terminated(self) -> list[Replica]:
        return list(self._terminated)

    def subscribe(self, cb: Callable[[str, Replica], None]) -> None:
        self._subscribers.append(cb)

    def _notify(self, kind: str, r: Replica) -> None:
        for cb in self._subscribers:
            try:
                cb(kind, r)
            except Exception:
                pass
