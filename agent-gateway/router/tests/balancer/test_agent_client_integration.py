import asyncio
import time
import pytest
from unittest.mock import MagicMock

import httpx
from router.src.balancer.circuit_breaker import CircuitBreaker
from router.src.balancer.hedging import LatencyTracker
from router.src.balancer.load_balancer import LoadBalancer
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.runtime import (
    clear_balancers, clear_passive_observers, reset_shared_http_client,
    set_balancer_for,
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


def test_agent_name_from_url_handles_kong_style_path():
    """Regression: in production the registry returns Kong gateway URLs
    (http://localhost:9100/agents/agent-a2a-translator), not direct-DNS.
    Hostname is `localhost`, so a hostname-only extractor would fall back
    to `localhost` and miss the balancer entirely. The extracted name
    must match the agent_name the DockerDiscoveryAdapter uses for its
    registry key, otherwise get_balancer_for(...) returns None and every
    request silently bypasses the balancer."""
    from router.src.core.agent_client import AgentClient
    # Strip exactly one prefix (agent- OR a2a-), matching
    # DockerDiscoveryAdapter._NAME_RE in discovery/docker_adapter.py.
    assert AgentClient._agent_name_from_url(
        "http://localhost:9100/agents/agent-a2a-translator"
    ) == "a2a-translator"
    assert AgentClient._agent_name_from_url(
        "http://localhost:9100/agents/agent-a2a-translator/invoke"
    ) == "a2a-translator"
    assert AgentClient._agent_name_from_url(
        "http://localhost:9100/agents/agent-github"
    ) == "github"
    assert AgentClient._agent_name_from_url(
        "http://localhost:9100/agents/a2a-translator"
    ) == "translator"


@pytest.mark.asyncio
async def test_send_request_consults_balancer_via_kong_url(monkeypatch):
    """End-to-end regression: a Kong-style production URL must route through
    the balancer (one of replicas a/b receives the request), NOT through
    Kong's legacy upstream. Catches the bug fixed in commit 4b97866 where
    _agent_name_from_url returned 'localhost' for Kong URLs and the
    balancer was silently bypassed."""
    clear_balancers()
    clear_passive_observers()

    a = Replica(id="a", agent_name="a2a-translator", container_name="agent-a2a-translator",
                addr="http://agent-a2a-translator:5000", status=ReplicaStatus.SERVING,
                joined_at=time.monotonic() - 1000)
    b = Replica(id="b", agent_name="a2a-translator", container_name="agent-a2a-translator-2",
                addr="http://agent-a2a-translator-2:5000", status=ReplicaStatus.SERVING,
                joined_at=time.monotonic() - 1000)
    cbs = {"agent-a2a-translator": CircuitBreaker(),
           "agent-a2a-translator-2": CircuitBreaker()}
    lb = LoadBalancer(
        "a2a-translator", _Reg([a, b]), RoundRobin(), cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    set_balancer_for("a2a-translator", lb)

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
    from router.src.core import agent_client as ac_mod
    monkeypatch.setattr(
        ac_mod.AgentClient, "_construct_payload",
        lambda self, request, files, agent_url: {"q": "x"},
    )

    client = ac_mod.AgentClient()
    # Kong-style URL as produced by the registry in production.
    for _ in range(4):
        await client.send_request(
            agent_url="http://localhost:9100/agents/agent-a2a-translator",
            request=MagicMock(), files=[], token="t",
        )

    # The balancer should have rewritten each call to the picked replica's
    # direct DNS address. If it hadn't, every call would land on
    # kong-gateway:8000 (the legacy single-URL path).
    assert all("kong-gateway" not in u for u in seen_urls), (
        f"balancer was bypassed; calls hit Kong instead of replicas: {seen_urls}"
    )
    # Both replicas should have received traffic via round-robin.
    hosts = {u.split("/")[2] for u in seen_urls}
    assert hosts == {"agent-a2a-translator:5000", "agent-a2a-translator-2:5000"}, (
        f"expected both replicas to receive traffic; got {hosts}"
    )


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


@pytest.mark.asyncio
async def test_send_request_falls_back_to_legacy_when_no_balancer(monkeypatch):
    """Regression for T10: when no LoadBalancer is registered for the agent,
    send_request must fall through to _legacy_send (which uses the original
    kong-gateway URL translation), not raise or silently drop."""
    clear_balancers()
    clear_passive_observers()

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

    from router.src.core import agent_client as ac_mod
    monkeypatch.setattr(
        ac_mod.AgentClient, "_construct_payload",
        lambda self, request, files, agent_url: {"q": "x"},
    )

    client = ac_mod.AgentClient()
    # http://localhost:9100/router/agent/unknown -> kong-gateway:8000 via legacy translate
    result = await client.send_request(
        agent_url="http://localhost:9100/router/agent/unknown",
        request=MagicMock(),
        files=[],
        token="t",
    )
    assert result == {"result": {"kind": "message"}}
    assert seen_urls == ["http://kong-gateway:8000/router/agent/unknown"], (
        f"legacy path must apply _translate_agent_url; got {seen_urls}"
    )


@pytest.mark.asyncio
async def test_send_request_hedges_when_enabled(monkeypatch):
    """Regression for T1: hedging must be reachable from the request path.
    With hedging_enabled and a primary that sleeps past hedge_after_s, the
    secondary should fire and the fast response should win."""
    clear_balancers()
    clear_passive_observers()

    now = time.monotonic() - 1000
    primary_rep = Replica(id="slow", agent_name="translator", container_name="slow",
                          addr="http://slow:5000", status=ReplicaStatus.SERVING,
                          joined_at=now)
    secondary_rep = Replica(id="fast", agent_name="translator", container_name="fast",
                            addr="http://fast:5000", status=ReplicaStatus.SERVING,
                            joined_at=now)
    cbs = {"slow": CircuitBreaker(), "fast": CircuitBreaker()}

    # Force pick_two -> (slow, fast) by using a strategy that always picks the
    # first candidate (slow is index 0; pick_two excludes it for secondary).
    class _AlwaysFirst:
        name = "first"
        def pick(self, candidates, routing_key=None):
            return candidates[0]

    lb = LoadBalancer(
        "translator", _Reg([primary_rep, secondary_rep]), _AlwaysFirst(), cbs,
        slow_start=SlowStart(window_seconds=0),
        hedging_enabled=True, latency_tracker=LatencyTracker(window=200),
    )
    # Pre-seed past the 20-sample warmup so p95_seconds returns the real
    # tail, not the 1.0s warmup default.
    for _ in range(30):
        lb.latency_tracker.observe(0.05)  # p95 ~50ms
    set_balancer_for("translator", lb)

    class _RaceClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *exc): return False
        async def post(self, url, json=None, headers=None, **kw):
            if "slow" in url:
                await asyncio.sleep(0.5)  # primary stalls
            else:
                await asyncio.sleep(0.01)  # secondary wins
            resp = MagicMock(status_code=200)
            resp.json = lambda: {"result": {"kind": "message", "from": url}}
            resp.raise_for_status = lambda: None
            return resp

    monkeypatch.setattr(httpx, "AsyncClient", _RaceClient)

    from router.src.core import agent_client as ac_mod
    monkeypatch.setattr(
        ac_mod.AgentClient, "_construct_payload",
        lambda self, request, files, agent_url: {"q": "x"},
    )

    # Explicit opt-in: idempotent=True is required since N3 flipped the
    # default to False. Mutating callers (e.g., create-PR) must NOT set
    # this; safe-to-retry reads do.
    req = MagicMock()
    req.idempotent = True
    client = ac_mod.AgentClient()
    start = time.perf_counter()
    result = await client.send_request(
        agent_url="http://a2a-translator:5000/invoke",
        request=req,
        files=[],
        token="t",
    )
    elapsed = time.perf_counter() - start
    assert "fast" in result["result"]["from"], (
        f"secondary should have won; got {result}"
    )
    assert elapsed < 0.4, (
        f"hedge should return in ~hedge_after_s + secondary_latency, not "
        f"wait for primary's 500ms; took {elapsed:.3f}s"
    )


@pytest.mark.asyncio
async def test_payload_construction_failure_does_not_leak_inflight(monkeypatch):
    """Regression for N1: payload is built before pick, so a payload
    construction error does NOT leave a reserved inflight slot pinned on
    a replica forever.
    """
    clear_balancers()
    clear_passive_observers()

    a = Replica(id="a", agent_name="translator", container_name="a",
                addr="http://a:5000", status=ReplicaStatus.SERVING,
                joined_at=time.monotonic() - 1000)
    cbs = {"a": CircuitBreaker()}
    lb = LoadBalancer(
        "translator", _Reg([a]), RoundRobin(), cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    set_balancer_for("translator", lb)

    from router.src.core import agent_client as ac_mod

    def _boom(self, request, files, agent_url):
        raise RuntimeError("payload boom")

    monkeypatch.setattr(ac_mod.AgentClient, "_construct_payload", _boom)

    client = ac_mod.AgentClient()
    with pytest.raises(RuntimeError, match="payload boom"):
        await client.send_request(
            agent_url="http://a2a-translator:5000/invoke",
            request=MagicMock(),
            files=[],
            token="t",
        )
    assert a.inflight == 0, (
        f"expected inflight to be released; got {a.inflight}"
    )


@pytest.mark.asyncio
async def test_hedging_does_not_fire_when_request_not_idempotent(monkeypatch):
    """Regression for N3: hedging defaults to OFF unless the request
    explicitly sets idempotent=True. A request without that attribute
    (or with it False) must take the single-replica path even when the
    LoadBalancer's hedging is enabled. Verifies the safety default for
    mutating tool calls.
    """
    clear_balancers()
    clear_passive_observers()

    primary_rep = Replica(id="p", agent_name="translator", container_name="p",
                          addr="http://p:5000", status=ReplicaStatus.SERVING,
                          joined_at=time.monotonic() - 1000)
    secondary_rep = Replica(id="s", agent_name="translator", container_name="s",
                            addr="http://s:5000", status=ReplicaStatus.SERVING,
                            joined_at=time.monotonic() - 1000)
    cbs = {"p": CircuitBreaker(), "s": CircuitBreaker()}

    lb = LoadBalancer(
        "translator", _Reg([primary_rep, secondary_rep]), RoundRobin(), cbs,
        slow_start=SlowStart(window_seconds=0),
        hedging_enabled=True, latency_tracker=LatencyTracker(window=200),
    )
    # Seed past warmup with a very low p95 so any hedge would fire instantly.
    for _ in range(30):
        lb.latency_tracker.observe(0.001)
    set_balancer_for("translator", lb)

    seen_urls: list[str] = []

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *exc): return False
        async def post(self, url, json=None, headers=None, **kw):
            seen_urls.append(url)
            await asyncio.sleep(0.05)
            resp = MagicMock(status_code=200)
            resp.json = lambda: {"result": {"kind": "message"}}
            resp.raise_for_status = lambda: None
            return resp

    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

    from router.src.core import agent_client as ac_mod
    monkeypatch.setattr(
        ac_mod.AgentClient, "_construct_payload",
        lambda self, request, files, agent_url: {"q": "x"},
    )

    # Plain request object: no idempotent attribute -> default False -> no hedge.
    class _PlainRequest:
        session_id = "abc"

    client = ac_mod.AgentClient()
    await client.send_request(
        agent_url="http://a2a-translator:5000/invoke",
        request=_PlainRequest(),
        files=[],
        token="t",
    )
    assert len(seen_urls) == 1, (
        f"hedging must not fire by default; saw {seen_urls}"
    )
    assert primary_rep.inflight == 0
    assert secondary_rep.inflight == 0


@pytest.mark.asyncio
async def test_send_request_reuses_shared_http_client(monkeypatch):
    """Regression for T9: once setup_shared_http_client has been called,
    every send_request must reuse the pooled client instead of opening a
    fresh one (which would cost a TCP handshake per call)."""
    clear_balancers()
    clear_passive_observers()
    reset_shared_http_client()

    a = Replica(id="a", agent_name="translator", container_name="a",
                addr="http://a:5000", status=ReplicaStatus.SERVING,
                joined_at=time.monotonic() - 1000)
    cbs = {"a": CircuitBreaker()}
    lb = LoadBalancer(
        "translator", _Reg([a]), RoundRobin(), cbs,
        slow_start=SlowStart(window_seconds=0),
    )
    set_balancer_for("translator", lb)

    construct_count = {"n": 0}
    post_calls: list[str] = []

    class _PooledClient:
        def __init__(self, *a, **kw):
            construct_count["n"] += 1
            self._closed = False

        async def post(self, url, json=None, headers=None, **kw):
            assert not self._closed, "pooled client used after close"
            post_calls.append(url)
            resp = MagicMock(status_code=200)
            resp.json = lambda: {"result": {"kind": "message"}}
            resp.raise_for_status = lambda: None
            return resp

        async def aclose(self):
            self._closed = True

        # Provide the async-context-manager hooks so the per-request fallback
        # path also works against this mock; the test asserts we don't take it.
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            await self.aclose()
            return False

    monkeypatch.setattr(httpx, "AsyncClient", _PooledClient)

    # Initialize the shared client via the same code path main.py uses.
    from router.src.balancer.runtime import (
        close_shared_http_client, setup_shared_http_client,
    )
    setup_shared_http_client(timeout=httpx.Timeout(5.0))
    assert construct_count["n"] == 1, "setup_shared_http_client should construct exactly one client"

    from router.src.core import agent_client as ac_mod
    monkeypatch.setattr(
        ac_mod.AgentClient, "_construct_payload",
        lambda self, request, files, agent_url: {"q": "x"},
    )

    client = ac_mod.AgentClient()
    for _ in range(5):
        await client.send_request(
            agent_url="http://a2a-translator:5000/invoke",
            request=MagicMock(),
            files=[],
            token="t",
        )

    assert construct_count["n"] == 1, (
        f"expected the pooled client to be reused; got "
        f"{construct_count['n']} AsyncClient constructions across 5 send_request calls"
    )
    assert len(post_calls) == 5
    await close_shared_http_client()
    reset_shared_http_client()
