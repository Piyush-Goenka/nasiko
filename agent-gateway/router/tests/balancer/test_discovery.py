from unittest.mock import MagicMock
from router.src.balancer.discovery.docker_adapter import DockerDiscoveryAdapter
from router.src.balancer.discovery.k8s_adapter import K8sDiscoveryAdapter


class _FakeContainer:
    def __init__(self, name, short_id, labels=None):
        self.name = name
        self.short_id = short_id
        self.labels = labels or {}


def test_docker_adapter_returns_raw_replicas():
    c = _FakeContainer("a2a-translator-1", "abc1", {"com.nasiko.agent": "translator"})
    network = MagicMock()
    network.containers = [c]
    docker = MagicMock()
    docker.networks.get.return_value = network

    adapter = DockerDiscoveryAdapter(docker_client=docker, network="agents-net")
    raws = adapter.list_replicas()
    assert len(raws) == 1
    assert raws[0].container_name == "a2a-translator-1"
    assert raws[0].addr == "http://a2a-translator-1:5000"
    assert raws[0].agent_name == "translator"


def test_docker_adapter_filters_infra():
    cs = [
        _FakeContainer("a2a-translator-1", "abc1", {"com.nasiko.agent": "translator"}),
        _FakeContainer("kong-gateway", "kong"),
        _FakeContainer("redis", "redis"),
    ]
    network = MagicMock()
    network.containers = cs
    docker = MagicMock()
    docker.networks.get.return_value = network
    adapter = DockerDiscoveryAdapter(docker_client=docker, network="agents-net")
    raws = adapter.list_replicas()
    assert [r.container_name for r in raws] == ["a2a-translator-1"]


def test_k8s_adapter_returns_raw_replicas():
    port = MagicMock(container_port=5000)
    container = MagicMock(ports=[port])
    pod = MagicMock()
    pod.metadata.name = "translator-deployment-78f-xyz"
    pod.metadata.uid = "uid-xyz"
    pod.metadata.labels = {"com.nasiko.agent": "translator"}
    pod.status.phase = "Running"
    pod.status.pod_ip = "10.0.0.5"
    pod.spec.containers = [container]

    core_v1 = MagicMock()
    core_v1.list_namespaced_pod.return_value.items = [pod]

    adapter = K8sDiscoveryAdapter(core_v1=core_v1, namespace="nasiko-agents")
    raws = adapter.list_replicas()
    assert len(raws) == 1
    assert raws[0].addr == "http://10.0.0.5:5000"
    assert raws[0].agent_name == "translator"


def test_k8s_adapter_skips_pending_pods():
    pod = MagicMock()
    pod.metadata.name = "translator-pending"
    pod.metadata.labels = {"com.nasiko.agent": "translator"}
    pod.status.phase = "Pending"
    pod.status.pod_ip = None
    pod.spec.containers = []

    core_v1 = MagicMock()
    core_v1.list_namespaced_pod.return_value.items = [pod]
    adapter = K8sDiscoveryAdapter(core_v1=core_v1, namespace="nasiko-agents")
    assert adapter.list_replicas() == []
