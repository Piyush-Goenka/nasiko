import asyncio
import logging
import pytest
from unittest.mock import MagicMock
from router.src.balancer.registry import InstanceRegistry
from router.src.balancer.discovery.docker_adapter import DockerDiscoveryAdapter
from router.src.balancer.models import ReplicaStatus


class _FakeContainer:
    def __init__(self, name, short_id, labels=None):
        self.name = name
        self.short_id = short_id
        self.labels = labels or {}


def _docker_adapter_with(containers):
    network = MagicMock()
    network.containers = containers
    client = MagicMock()
    client.networks.get.return_value = network
    return DockerDiscoveryAdapter(docker_client=client, network="agents-net")


def test_registry_discovers_agent_containers():
    # Nasiko reference agents use "a2a-<name>-<n>" when scaled via docker compose
    containers = [
        _FakeContainer("a2a-translator-1", "abc1", {"com.nasiko.agent": "translator"}),
        _FakeContainer("a2a-translator-2", "abc2", {"com.nasiko.agent": "translator"}),
        _FakeContainer("kong-gateway", "kong1"),
        _FakeContainer("redis", "redis1"),
    ]
    reg = InstanceRegistry(adapter=_docker_adapter_with(containers))
    reg.refresh_sync()
    replicas = reg.replicas_for("translator")
    assert {r.container_name for r in replicas} == {"a2a-translator-1", "a2a-translator-2"}
    assert all(r.status is ReplicaStatus.DISCOVERED for r in replicas)


def test_registry_parses_agent_name_from_container_name_when_label_missing():
    # Test both legacy "agent-" prefix and current "a2a-" prefix
    containers = [
        _FakeContainer("agent-github-7", "g7"),
        _FakeContainer("a2a-translator-3", "t3"),
    ]
    reg = InstanceRegistry(adapter=_docker_adapter_with(containers))
    reg.refresh_sync()
    assert {r.agent_name for r in reg.all_replicas()} == {"github", "translator"}


def test_subscriber_exception_does_not_break_delivery_to_others(caplog):
    """Regression for I8: a misbehaving subscriber must not block notifications
    to other subscribers, and the failure must surface in the logs."""
    container = _FakeContainer(
        "a2a-translator-1", "abc1", {"com.nasiko.agent": "translator"},
    )
    reg = InstanceRegistry(adapter=_docker_adapter_with([container]))
    delivered = []

    def _bad(kind, r):
        raise RuntimeError("subscriber boom")

    def _good(kind, r):
        delivered.append((kind, r.container_name))

    reg.subscribe(_bad)
    reg.subscribe(_good)
    with caplog.at_level(logging.WARNING, logger="router.src.balancer.registry"):
        reg.refresh_sync()
    assert ("added", "a2a-translator-1") in delivered
    assert any("subscriber raised" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_stop_awaits_cancelled_task():
    """Regression for I9: stop() must cancel AND await the refresh task so
    shutdown does not produce 'Task was destroyed but it is pending' warnings."""

    class _SlowAdapter:
        def list_replicas(self):
            return []

    reg = InstanceRegistry(adapter=_SlowAdapter(), refresh_interval=0.01)
    await reg.start()
    await asyncio.sleep(0.05)  # let one refresh fire
    await reg.stop()
    assert reg._task is None, "stop() must clear the task handle"


@pytest.mark.asyncio
async def test_run_loop_logs_refresh_failures(caplog):
    """Regression for I8: refresh errors must log instead of silently passing."""

    class _BoomAdapter:
        calls = 0

        def list_replicas(self):
            type(self).calls += 1
            raise RuntimeError("docker socket down")

    reg = InstanceRegistry(adapter=_BoomAdapter(), refresh_interval=0.01)
    with caplog.at_level(logging.WARNING, logger="router.src.balancer.registry"):
        await reg.start()
        await asyncio.sleep(0.05)
        await reg.stop()
    assert _BoomAdapter.calls >= 1
    assert any("registry refresh failed" in rec.message for rec in caplog.records)


def test_registry_marks_disappeared_replicas_terminated():
    c1 = _FakeContainer("a2a-translator-1", "abc1", {"com.nasiko.agent": "translator"})
    c2 = _FakeContainer("a2a-translator-2", "abc2", {"com.nasiko.agent": "translator"})
    network = MagicMock()
    network.containers = [c1, c2]
    client = MagicMock()
    client.networks.get.return_value = network
    adapter = DockerDiscoveryAdapter(docker_client=client, network="agents-net")
    reg = InstanceRegistry(adapter=adapter)
    reg.refresh_sync()
    # simulate c2 disappearing
    client.networks.get.return_value.containers = [c1]
    reg.refresh_sync()
    all_t = reg.replicas_for("translator")
    by_status = {r.container_name: r.status for r in all_t + reg.terminated()}
    assert by_status["a2a-translator-2"] is ReplicaStatus.TERMINATED
