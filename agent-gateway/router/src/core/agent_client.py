"""
Agent client service for communicating with selected agents.
"""

import logging
import time
from typing import Dict, List, Tuple, Any
from urllib.parse import urlparse

import httpx
from router.src.config import settings
from router.src.entities import UserRequest
from router.src.balancer import metrics, tracing
from router.src.balancer.load_balancer import NoHealthyReplica
from router.src.balancer.runtime import get_balancer_for, passive_observe_hook

logger = logging.getLogger(__name__)


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
        http://a2a-translator-1:5000/...  -> "translator"
        http://agent-github-7:5000/...    -> "github" (legacy prefix)
        Falls back to the hostname when no recognised prefix matches.
        """
        host = urlparse(agent_url).hostname or ""
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

        try:
            t0 = time.perf_counter()
            replica = lb.pick()
            metrics.route_decision_us.labels(agent_name, lb.strategy_name).observe(
                (time.perf_counter() - t0) * 1_000_000
            )
        except NoHealthyReplica as e:
            raise AgentClientError(f"no healthy replicas for {agent_name}: {e}") from e

        replica.inflight += 1
        metrics.inflight.labels(agent_name, replica.container_name).inc()
        span = tracing.start_route_span(agent_name, lb.strategy_name, replica)

        headers: Dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        tracing.inject_traceparent(span, headers)

        target = self._rewrite_for_replica(agent_url, replica.addr)
        payload = self._construct_payload(request, files, target)
        t_req = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(target, json=payload, headers=headers)
                response.raise_for_status()
            latency_s = time.perf_counter() - t_req
            sc = metrics.status_class(response.status_code)
            metrics.requests_total.labels(agent_name, replica.container_name, sc).inc()
            metrics.request_duration_seconds.labels(
                agent_name, replica.container_name
            ).observe(latency_s)
            passive_observe_hook(replica, response.status_code, latency_s * 1000)

            data = response.json()
            if "error" in data:
                raise AgentClientError(f"Agent error: {data['error']}")
            if "result" not in data:
                raise AgentClientError("Invalid response: missing 'result' field")
            return data

        except httpx.HTTPStatusError as e:
            latency_s = time.perf_counter() - t_req
            metrics.requests_total.labels(agent_name, replica.container_name, "5xx").inc()
            passive_observe_hook(replica, e.response.status_code, latency_s * 1000)
            error_msg = (
                f"HTTP error from {replica.container_name}: "
                f"{e.response.status_code} - {e.response.text}"
            )
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e

        except httpx.RequestError as e:
            metrics.requests_total.labels(agent_name, replica.container_name, "5xx").inc()
            passive_observe_hook(
                replica, 500, (time.perf_counter() - t_req) * 1000
            )
            error_msg = f"Request error to {replica.container_name}: {e}"
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e

        except AgentClientError:
            # Pre-wrapped error from payload validation above; propagate as-is.
            raise

        except Exception as e:
            metrics.requests_total.labels(agent_name, replica.container_name, "5xx").inc()
            passive_observe_hook(
                replica, 500, (time.perf_counter() - t_req) * 1000
            )
            error_msg = f"Unexpected error communicating with {replica.container_name}: {e}"
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e

        finally:
            replica.inflight -= 1
            metrics.inflight.labels(agent_name, replica.container_name).dec()
            span.end()

    async def _legacy_send(
        self,
        agent_url: str,
        request: UserRequest,
        files: List[Tuple[str, Tuple[str, bytes, str]]],
        token: str,
    ) -> Dict[str, Any]:
        """Original send_request behavior used when no balancer is registered."""
        try:
            translated_url = self._translate_agent_url(agent_url)
            payload = self._construct_payload(request, files, translated_url)

            logger.info(f"Sending request to agent: {agent_url} -> {translated_url}")
            logger.debug(f"Payload: {payload}")

            headers = {}
            if token:
                headers["Authorization"] = f"Bearer {token}"

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    translated_url, json=payload, headers=headers
                )
                response.raise_for_status()

            data = response.json()
            if "error" in data:
                raise AgentClientError(f"Agent error: {data['error']}")
            if "result" not in data:
                raise AgentClientError("Invalid response: missing 'result' field")

            logger.info("Successfully received response from agent")
            return data

        except httpx.HTTPStatusError as e:
            error_msg = (
                f"HTTP error from agent {translated_url}: "
                f"{e.response.status_code} - {e.response.text}"
            )
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e

        except httpx.RequestError as e:
            error_msg = f"Request error to agent {translated_url}: {e}"
            logger.error(error_msg)
            raise AgentClientError(error_msg) from e

        except AgentClientError:
            raise

        except Exception as e:
            error_msg = (
                f"Unexpected error communicating with agent {translated_url}: {e}"
            )
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

        except Exception as e:
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

        except Exception as e:
            logger.warning(f"Health check failed for agent {agent_url}: {e}")
            return False
