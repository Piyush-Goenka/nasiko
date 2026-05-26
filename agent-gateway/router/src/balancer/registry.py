import asyncio
import time
from typing import Callable
from .discovery.base import DiscoveryAdapter
from .models import Replica, ReplicaStatus


class InstanceRegistry:
    """
    Maintains the live pool of agent replicas discovered via an injected
    DiscoveryAdapter (Docker SDK or Kubernetes API).

    Replicas walk: DISCOVERED -> READY -> SERVING -> DRAINING -> TERMINATED.
    A periodic refresh detects new replicas and marks missing ones terminated.
    Subscribers are notified on add/remove for downstream wiring (health probes,
    load-balancer pool updates, dashboard events).
    """

    def __init__(self, adapter: DiscoveryAdapter, refresh_interval: float = 5.0):
        self._adapter = adapter
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
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(self._interval)

    async def refresh(self) -> None:
        """
        Async refresh entry point used by the background loop.

        Discovery adapters call the Docker socket or the Kubernetes API,
        both of which are blocking I/O (typically 50-200ms per round trip).
        Running them inline would stall the event loop and every routing
        decision for that duration. We offload only the blocking adapter
        call to a worker thread, then apply state changes and fire
        subscriber callbacks back on the event loop so they can safely
        call `asyncio.create_task`.
        """
        raws = await asyncio.to_thread(self._adapter.list_replicas)
        self._apply(raws)

    def refresh_sync(self) -> None:
        """
        Synchronous refresh, retained for unit tests that pass mocked
        adapters with negligible work. Production goes through `refresh`.
        """
        self._apply(self._adapter.list_replicas())

    def _apply(self, raws) -> None:
        seen: set[str] = set()
        for raw in raws:
            seen.add(raw.container_name)
            if raw.container_name not in self._replicas:
                r = Replica(
                    id=raw.id,
                    agent_name=raw.agent_name,
                    container_name=raw.container_name,
                    addr=raw.addr,
                    status=ReplicaStatus.DISCOVERED,
                    joined_at=time.monotonic(),
                )
                self._replicas[raw.container_name] = r
                self._notify("added", r)
        for name in list(self._replicas):
            if name not in seen:
                r = self._replicas.pop(name)
                r.status = ReplicaStatus.TERMINATED
                self._terminated.append(r)
                self._notify("removed", r)

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
