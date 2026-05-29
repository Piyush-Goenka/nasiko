import asyncio
import logging
import math
import time
import urllib.parse
from typing import Awaitable, Callable
from .circuit_breaker import CircuitBreaker
from .models import Replica, ReplicaStatus

logger = logging.getLogger(__name__)

EWMA_TAU_SECONDS = 30.0


class HealthChecker:
    """
    Combined active + passive health detection.

    - Active: periodic /health probe (with A2A and TCP fallback) promotes
      DISCOVERED -> READY -> SERVING. Two consecutive failures trip the breaker.
    - Passive: every response from a replica is observed; 5xx/timeout
      increments consecutive_5xx and trips the breaker at failure_threshold.
      Successful responses reset the counter and feed an EWMA latency gauge.
    """

    def __init__(self,
                 breakers: dict[str, CircuitBreaker],
                 http_get: Callable[..., Awaitable] | None = None,
                 interval: float = 5.0,
                 timeout: float = 2.0,
                 active_failure_threshold: int = 2,
                 on_status_change: Callable[[Replica, ReplicaStatus, ReplicaStatus], None] | None = None,
                 on_probe_failed: Callable[[Replica], None] | None = None):
        self._breakers = breakers
        self._http_get = http_get
        self._interval = interval
        self._timeout = timeout
        self._active_threshold = active_failure_threshold
        self._last_observed_at: dict[str, float] = {}
        # Observer hooks. main.py wires these into the EventBroker so that
        # DISCOVERED -> READY -> SERVING transitions become replica_ready /
        # replica_serving SSE events and probe failures become
        # health_check_failed events. Errors in observers must never break
        # the probe loop, so callers are wrapped in try/except.
        self._on_status_change = on_status_change
        self._on_probe_failed = on_probe_failed

    def passive_observe(self, replica: Replica, status_code: int,
                        latency_ms: float) -> None:
        now = time.monotonic()
        last = self._last_observed_at.get(replica.container_name, now)
        dt = max(now - last, 1e-6)
        self._last_observed_at[replica.container_name] = now
        alpha = 1.0 - math.exp(-dt / EWMA_TAU_SECONDS)
        if replica.ewma_latency_ms == 0.0:
            replica.ewma_latency_ms = latency_ms
        else:
            replica.ewma_latency_ms = alpha * latency_ms + (1 - alpha) * replica.ewma_latency_ms
        breaker = self._breakers.get(replica.container_name)
        if 500 <= status_code < 600:
            replica.consecutive_5xx += 1
            if breaker is not None:
                breaker.on_failure()
        elif 200 <= status_code < 300:
            replica.consecutive_5xx = 0
            if breaker is not None:
                breaker.on_success()
        else:
            # 3xx/4xx are not a replica fault. Reset the consecutive-5xx
            # counter so a 4xx after some 5xx errors does not artificially
            # keep the count climbing, but do NOT call breaker.on_success():
            # a 4xx is not evidence the replica is healthy, and treating it
            # that way would silently "heal" a HALF_OPEN breaker via client
            # errors (auth failures, validation rejects, missing routes).
            replica.consecutive_5xx = 0

    async def run_probe(self, replica: Replica, stop_after: float | None = None) -> None:
        start = time.monotonic()
        while True:
            if stop_after is not None and time.monotonic() - start >= stop_after:
                return
            # Self-exit if the registry marked the replica gone. Belt-and-braces
            # with the explicit task cancel in main.py: covers any path where
            # the replica is removed but the spawning task wasn't tracked.
            if replica.status is ReplicaStatus.TERMINATED:
                return
            ok = await self._probe_once(replica)
            breaker = self._breakers.get(replica.container_name)
            if ok:
                replica.consecutive_health_failures = 0
                if replica.status is ReplicaStatus.DISCOVERED:
                    self._transition(replica, ReplicaStatus.READY)
                elif replica.status is ReplicaStatus.READY:
                    self._transition(replica, ReplicaStatus.SERVING)
            else:
                replica.consecutive_health_failures += 1
                self._fire_probe_failed(replica)
                if breaker is not None and replica.consecutive_health_failures >= self._active_threshold:
                    breaker.on_failure()
            await asyncio.sleep(self._interval)

    def _transition(self, replica: Replica, new_status: ReplicaStatus) -> None:
        old = replica.status
        replica.status = new_status
        if self._on_status_change is not None:
            try:
                self._on_status_change(replica, old, new_status)
            except Exception:
                pass

    def _fire_probe_failed(self, replica: Replica) -> None:
        if self._on_probe_failed is not None:
            try:
                self._on_probe_failed(replica)
            except Exception:
                pass

    async def _probe_once(self, replica: Replica) -> bool:
        """
        A2A reference agents do NOT expose /health uniformly. Probe order:
          1. GET /health (Nasiko router convention)
          2. GET /.well-known/agent-card (A2A spec convention; 200 means ready)
          3. TCP connect on port 5000 (liveness only)
        First success wins; the replica is marked healthy.
        """
        if self._http_get is None:
            # No probe wired: warn once per replica so a misconfigured
            # production deploy is visible in the logs rather than silently
            # marking everything healthy forever. Tests that intentionally
            # pass http_get=None just see the warning and move on.
            if not getattr(replica, "_warned_no_http_get", False):
                logger.warning(
                    "HealthChecker.http_get is None for %s; treating as healthy. "
                    "Wire an http_get to enable real probing.",
                    replica.container_name,
                )
                replica._warned_no_http_get = True  # type: ignore[attr-defined]
            return True
        for path in ("/health", "/.well-known/agent-card.json", "/.well-known/agent.json", "/.well-known/agent-card"):
            try:
                resp = await self._http_get(replica.addr + path, timeout=self._timeout)
                if getattr(resp, "status_code", 500) == 200:
                    return True
            except Exception:
                continue
        # TCP fallback
        try:
            host = urllib.parse.urlparse(replica.addr).hostname
            port = urllib.parse.urlparse(replica.addr).port or 5000
            _, w = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=self._timeout,
            )
            w.close()
            return True
        except Exception:
            return False
