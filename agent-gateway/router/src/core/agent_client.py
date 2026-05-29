"""
Agent client service for communicating with selected agents.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from router.src.config import settings
from router.src.entities import UserRequest
from router.src.balancer import metrics, tracing
from router.src.balancer.hedging import hedge_request
from router.src.balancer.load_balancer import LoadBalancer, NoHealthyReplica
from router.src.balancer.runtime import (
    get_balancer_for, get_shared_http_client, passive_observe_hook,
)

logger = logging.getLogger(__name__)

# Exception classes whose meaning is "transport failure" — these belong on the
# 5xx counter and the passive-observe hook. Anything else (KeyError,
# JSONDecodeError, AttributeError, ...) is a programming error and must surface
# as such rather than masquerading as a transport failure.
_TRANSPORT_EXC: Tuple[type, ...] = (
    httpx.HTTPError,
    asyncio.TimeoutError,
    OSError,
)


class AgentClientError(Exception):
    """Custom exception for agent client errors."""

    pass


class AgentClient:
    """Service for communicating with agents."""

    def __init__(self):
        self.timeout = httpx.Timeout(settings.REQUEST_TIMEOUT)

    def _translate_agent_url(self, agent_url: str) -> str:
        """
        Translate external agent URLs to internal Docker network URLs for local deployment.
        """
        if "localhost:9100" in agent_url:
            return agent_url.replace("localhost:9100", "kong-gateway:8000")
        return agent_url

    @staticmethod
    def _agent_name_from_url(agent_url: str) -> str:
        """
        Extract the logical agent name from a URL.
        http://a2a-translator-1:5000/...                          -> "translator"
        http://agent-github-7:5000/...                            -> "github" (legacy prefix)
        http://localhost:9100/agents/agent-a2a-translator/...     -> "translator" (Kong URL)
        Falls back to the hostname when no recognised prefix matches.
        """
        parsed = urlparse(agent_url)
        host = parsed.hostname or ""

        # Kong-style URL: agent name lives in the path under /agents/
        # Strip exactly one prefix (agent- OR a2a-) to mirror DockerDiscoveryAdapter._NAME_RE.
        path_parts = [p for p in (parsed.path or "").split("/") if p]
        if len(path_parts) >= 2 and path_parts[0] == "agents":
            candidate = path_parts[1]
            for prefix in ("a2a-", "agent-"):
                if candidate.startswith(prefix):
                    parts = candidate[len(prefix):].split("-")
                    if parts and parts[-1].isdigit():
                        parts = parts[:-1]
                    return "-".join(parts) if parts else candidate
            return candidate

        # Direct-DNS URL (eg http://a2a-translator-1:5000/...)
        for prefix in ("a2a-", "agent-"):
            if host.startswith(prefix):
                parts = host[len(prefix):].split("-")
                if parts and parts[-1].isdigit():
                    parts = parts[:-1]
                return "-".join(parts) if parts else host
        return host

    def _rewrite_for_replica(self, original_url: str, replica_addr: str) -> str:
        """Replace scheme+host+port of `original_url` with `replica_addr`'s, keep path."""
        p = urlparse(original_url)
        path = p.path or ""
        if p.query:
            path = f"{path}?{p.query}"
        return f"{replica_addr.rstrip('/')}{path}"

    @staticmethod
    def _routing_key_for(request: UserRequest) -> Optional[str]:
        for attr in ("session_id", "user_id", "conversation_id"):
            value = getattr(request, attr, None)
            if value:
                return str(value)
        return None

    async def _post_and_validate(
        self,
        url: str,
        payload: Dict[str, Any],
        headers: Dict[str, str],
    ) -> Tuple[Dict[str, Any], int]:
        """
        Single source of truth for the HTTP call + response shape validation.
        Reuses the process-wide pooled httpx.AsyncClient when available (set
        up at FastAPI startup); otherwise opens a fresh client per request.
        The pooled path cuts ~one TCP handshake per request under load.
        Raises:
          httpx.HTTPStatusError / httpx.RequestError: transport-layer failures
          AgentClientError: agent-reported "error" field or malformed shape
        """
        shared = get_shared_http_client()
        if shared is not None:
            response = await shared.post(url, json=payload, headers=headers)
            response.raise_for_status()
        else:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise AgentClientError(f"Agent error: {data['error']}")
        if "result" not in data:
            raise AgentClientError("Invalid response: missing 'result' field")
        return data, response.status_code

    async def send_request(
        self,
        agent_url: str,
        request: UserRequest,
        files: List[Tuple[str, Tuple[str, bytes, str]]],
        token: str,
    ) -> Dict[str, Any]:
        """
        Send a request to an agent. Consults the LoadBalancer when one is
        registered for the agent (replicas are auto-discovered). Falls back
        to the legacy single-URL path when no balancer exists.

        Raises:
            AgentClientError: on any HTTP, transport, or pool-exhaustion failure.
        """
        agent_name = self._agent_name_from_url(agent_url)
        lb = get_balancer_for(agent_name)
        if lb is None:
            return await self._legacy_send(agent_url, request, files, token)

        routing_key = self._routing_key_for(request)
        # Build payload BEFORE pick so a construction error cannot leak the
        # pre-incremented inflight slot. If payload construction fails after
        # a successful pick, the reservation lingers until the next refresh
        # because no finally clause runs.
        headers: Dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        payload = self._construct_payload(request, files, agent_url)

        try:
            t0 = time.perf_counter()
            replica, pick_stats = lb.pick_with_stats(routing_key=routing_key)
            decision_us = (time.perf_counter() - t0) * 1_000_000
            metrics.route_decision_us.labels(agent_name, lb.strategy_name).observe(
                decision_us
            )
            pick_stats["decision_latency_us"] = decision_us
        except NoHealthyReplica as e:
            raise AgentClientError(f"no healthy replicas for {agent_name}: {e}") from e

        # From here on, `replica.inflight` is reserved and MUST be released
        # by exactly one finally clause (_lb_send / _hedge_send) regardless
        # of which control-flow branch we take. Any code added between this
        # pick and the dispatch must propagate exceptions through a try/
        # finally that decrements on the unwind path, or the slot leaks.
        try:
            # Hedging path: requests must explicitly opt in via
            # `idempotent=True`. The default is OFF because mutating tool
            # calls (e.g., github-agent create-PR) that forget to set the
            # flag would otherwise be hedged and double-execute. Callers
            # who know their request is safe to retry set idempotent=True.
            secondary = None
            if (
                lb.hedging_enabled
                and lb.latency_tracker is not None
                and getattr(request, "idempotent", False)
            ):
                secondary = lb.pick_secondary(replica, routing_key=routing_key)
        except Exception:
            replica.inflight -= 1
            raise

        if secondary is not None:
            hedge_after_s = lb.latency_tracker.p95_seconds()
            return await self._hedge_send(
                lb=lb,
                agent_name=agent_name,
                primary=replica,
                secondary=secondary,
                agent_url=agent_url,
                payload=payload,
                base_headers=headers,
                hedge_after_s=hedge_after_s,
                primary_pick_stats=pick_stats,
            )

        return await self._lb_send(
            lb=lb, agent_name=agent_name, replica=replica,
            agent_url=agent_url, payload=payload, base_headers=headers,
            pick_stats=pick_stats,
        )

    async def _lb_send(
        self,
        *,
        lb: LoadBalancer,
        agent_name: str,
        replica,
        agent_url: str,
        payload: Dict[str, Any],
        base_headers: Dict[str, str],
        pick_stats: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Single-replica path: instrument, dispatch, record latency.

        `replica.inflight` was already incremented by `LoadBalancer.pick_with_stats`
        (or `pick_secondary` on the hedge path) so the strategy's view stayed
        consistent during selection. We only mirror the Prometheus gauge here
        and decrement both in the finally clause.
        """
        metrics.inflight.labels(agent_name, replica.container_name).inc()
        span_kwargs = {}
        if pick_stats:
            for k in (
                "pool_size", "healthy_count", "decision_latency_us",
                "candidates_considered", "inflight_at_selection",
            ):
                if k in pick_stats:
                    span_kwargs[k] = pick_stats[k]
        span = tracing.start_route_span(
            agent_name, lb.strategy_name, replica, **span_kwargs,
        )
        headers = dict(base_headers)
        tracing.inject_traceparent(span, headers)

        target = self._rewrite_for_replica(agent_url, replica.addr)
        t_req = time.perf_counter()
        try:
            data, status_code = await self._post_and_validate(target, payload, headers)
        except httpx.HTTPStatusError as e:
            self._record_transport_failure(
                agent_name, replica, t_req, e.response.status_code,
                f"HTTP error from {replica.container_name}: "
                f"{e.response.status_code} - {e.response.text}",
            )
            raise AgentClientError(
                f"HTTP error from {replica.container_name}: "
                f"{e.response.status_code} - {e.response.text}"
            ) from e
        except _TRANSPORT_EXC as e:
            self._record_transport_failure(
                agent_name, replica, t_req, 500,
                f"Transport error to {replica.container_name}: {e!r}",
            )
            raise AgentClientError(
                f"Request error to {replica.container_name}: {e}"
            ) from e
        except AgentClientError:
            # Agent-reported error from _post_and_validate; do NOT pretend it
            # was a transport failure. Record the latency only.
            latency_s = time.perf_counter() - t_req
            metrics.request_duration_seconds.labels(
                agent_name, replica.container_name
            ).observe(latency_s)
            raise
        else:
            latency_s = time.perf_counter() - t_req
            sc = metrics.status_class(status_code)
            metrics.requests_total.labels(agent_name, replica.container_name, sc).inc()
            metrics.request_duration_seconds.labels(
                agent_name, replica.container_name
            ).observe(latency_s)
            metrics.request_counter.record(agent_name, replica.container_name)
            passive_observe_hook(replica, status_code, latency_s * 1000)
            if lb.latency_tracker is not None:
                lb.latency_tracker.observe(latency_s)
            return data
        finally:
            replica.inflight -= 1
            metrics.inflight.labels(agent_name, replica.container_name).dec()
            span.end()

    async def _hedge_send(
        self,
        *,
        lb: LoadBalancer,
        agent_name: str,
        primary,
        secondary,
        agent_url: str,
        payload: Dict[str, Any],
        base_headers: Dict[str, str],
        hedge_after_s: float,
        primary_pick_stats: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Hedged path: race primary against a secondary fired after hedge_after_s.

        Both `primary` and `secondary` already have their inflight pre-incremented
        by the LoadBalancer. The two `_lb_send` calls handle the matching
        decrements in their finally clauses. The secondary may never fire (primary
        wins under hedge_after_s); we release its reservation in the outer finally
        so the in-memory inflight gauge does not drift upward forever.
        """
        # Bind in defaults so lambda closure captures the right replicas, not
        # whichever value `primary`/`secondary` happen to hold when the lambda
        # is invoked. Defensive against any future caller refactor.
        secondary_dispatched = False

        async def _call_primary(_r=primary, _stats=primary_pick_stats):
            return await self._lb_send(
                lb=lb, agent_name=agent_name, replica=_r,
                agent_url=agent_url, payload=payload, base_headers=base_headers,
                pick_stats=_stats,
            )

        async def _call_secondary(_r=secondary):
            nonlocal secondary_dispatched
            secondary_dispatched = True
            return await self._lb_send(
                lb=lb, agent_name=agent_name, replica=_r,
                agent_url=agent_url, payload=payload, base_headers=base_headers,
            )

        try:
            return await hedge_request(
                primary=_call_primary,
                secondary=_call_secondary,
                hedge_after_s=hedge_after_s,
            )
        finally:
            if not secondary_dispatched:
                # Primary finished before the hedge trigger; release the
                # secondary's reserved slot so strategy decisions and the
                # Prometheus gauge stay accurate.
                secondary.inflight -= 1

    def _record_transport_failure(
        self,
        agent_name: str,
        replica,
        t_req: float,
        status_code: int,
        log_msg: str,
    ) -> None:
        latency_s = time.perf_counter() - t_req
        sc = metrics.status_class(status_code) if status_code >= 400 else "5xx"
        metrics.requests_total.labels(agent_name, replica.container_name, sc).inc()
        metrics.request_duration_seconds.labels(
            agent_name, replica.container_name
        ).observe(latency_s)
        metrics.request_counter.record(agent_name, replica.container_name)
        passive_observe_hook(replica, status_code, latency_s * 1000)
        logger.error(log_msg)

    async def _legacy_send(
        self,
        agent_url: str,
        request: UserRequest,
        files: List[Tuple[str, Tuple[str, bytes, str]]],
        token: str,
    ) -> Dict[str, Any]:
        """Original send_request behavior used when no balancer is registered."""
        translated_url = self._translate_agent_url(agent_url)
        payload = self._construct_payload(request, files, translated_url)

        logger.info(f"Sending request to agent: {agent_url} -> {translated_url}")
        logger.debug(f"Payload: {payload}")

        headers: Dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            data, _ = await self._post_and_validate(translated_url, payload, headers)
            logger.info("Successfully received response from agent")
            return data
        except httpx.HTTPStatusError as e:
            error_msg = (
                f"HTTP error from agent {translated_url}: "
                f"{e.response.status_code} - {e.response.text}"
            )
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e
        except _TRANSPORT_EXC as e:
            error_msg = f"Request error to agent {translated_url}: {e}"
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e

    def _construct_payload(
        self,
        request: UserRequest,
        files: List[Tuple[str, Tuple[str, bytes, str]]],
        agent_url: str,
    ) -> Dict[str, Any]:
        """
        Construct the payload for agent request.
        """
        from router.src.utils import construct_payload

        return construct_payload(request, files, agent_url)

    def extract_response_content(self, agent_data: Dict[str, Any]) -> str:
        """
        Extract the text content from agent response.
        """
        try:
            result = agent_data.get("result")

            if not result:
                raise AgentClientError("Invalid response: missing 'result' field")

            kind = result.get("kind")

            if kind == "message":
                return self._extract_text_from_message(result)
            elif kind == "task":
                artifacts = result.get("artifacts", [])
                if not artifacts:
                    raise AgentClientError("Task returned with empty history")
                last_msg = artifacts[-1]
                return self._extract_text_from_message(last_msg)
            else:
                raise AgentClientError(f"Unknown response kind: {kind}")

        except AgentClientError:
            raise
        except (KeyError, AttributeError, TypeError) as e:
            error_msg = f"Failed to extract response content: {e}"
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e

    def _extract_text_from_message(self, message: Dict[str, Any]) -> str:
        """
        Extract text from a message object.
        """
        from router.src.utils import extract_text_from_message

        return extract_text_from_message(message)

    async def health_check(self, agent_url: str) -> bool:
        """
        Check if an agent is healthy and responding.
        """
        try:
            translated_url = self._translate_agent_url(agent_url)
            health_url = f"{translated_url.rstrip('/')}/health"

            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                response = await client.get(health_url)
                return response.status_code == 200

        except (httpx.HTTPError, asyncio.TimeoutError, OSError) as e:
            logger.warning(f"Health check failed for agent {agent_url}: {e}")
            return False
