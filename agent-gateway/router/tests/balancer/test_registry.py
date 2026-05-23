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
