import time
import pytest
from unittest.mock import MagicMock

import httpx
from router.src.balancer.circuit_breaker import CircuitBreaker
from router.src.balancer.load_balancer import LoadBalancer
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.runtime import (
    clear_balancers, clear_passive_observers, set_balancer_for,
)
from router.src.balancer.slow_start import SlowStart
from router.src.balancer.strategies.round_robin import RoundRobin


class _Reg:
    def __init__(self, replicas):
        self.r = replicas

    def replicas_for(self, _):
        return self.r


def _agent_name_extraction_smoke():
    """Quick sanity check the URL-parsing helper handles the real container names."""
    from router.src.core.agent_client import AgentClient
    assert AgentClient._agent_name_from_url("http://a2a-translator-1:5000/invoke") == "translator"
    assert AgentClient._agent_name_from_url("http://agent-github-7:5000/x") == "github"


def test_agent_name_from_url_handles_a2a_and_agent_prefixes():
    _agent_name_extraction_smoke()


@pytest.mark.asyncio
async def test_send_request_consults_load_balancer(monkeypatch):
    clear_balancers()
    clear_passive_observers()

    a = Replica(id="a", agent_name="translator", container_name="a",
                addr="http://a:5000", status=ReplicaStatus.SERVING,
                joined_at=time.monotonic() - 1000)
    b = Replica(id="b", agent_name="translator", container_name="b",
                addr="http://b:5000", status=ReplicaStatus.SERVING,
                joined_at=time.monotonic() - 1000)
    cbs = {"a": CircuitBreaker(), "b": CircuitBreaker()}
    lb = LoadBalancer(
        "translator", _Reg([a, b]), RoundRobin(), cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    set_balancer_for("translator", lb)

    seen_urls: list[str] = []

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *exc): return False
        async def post(self, url, json=None, headers=None, **kw):
            seen_urls.append(url)
            resp = MagicMock(status_code=200)
            resp.json = lambda: {"result": {"kind": "message"}}
            resp.raise_for_status = lambda: None
            return resp

    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

    # Patch the payload construction so we don't pull in the real router
    # entities subgraph (which requires app config that may not be set in CI).
    from router.src.core import agent_client as ac_mod
    monkeypatch.setattr(
        ac_mod.AgentClient, "_construct_payload",
        lambda self, request, files, agent_url: {"q": "x"},
    )

    client = ac_mod.AgentClient()
    await client.send_request(
        agent_url="http://a2a-translator:5000/invoke",
        request=MagicMock(),
        files=[],
        token="t",
    )
    await client.send_request(
        agent_url="http://a2a-translator:5000/invoke",
        request=MagicMock(),
        files=[],
        token="t",
    )

    assert seen_urls == ["http://a:5000/invoke", "http://b:5000/invoke"]
