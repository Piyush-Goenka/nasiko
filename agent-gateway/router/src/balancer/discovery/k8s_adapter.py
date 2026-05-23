from .base import RawReplica


class K8sDiscoveryAdapter:
    """
    Kubernetes pod discovery. Mirrors the contract in
    agent-gateway/registry/registry.py for K8S_ENABLED=true mode.

    Filters Running pods with the label_selector and reports the pod IP
    on the agent port (default 5000). The agent_name is taken from the
    `com.nasiko.agent` label.
    """

    def __init__(self, core_v1, namespace: str = "nasiko-agents",
                 label_selector: str = "com.nasiko.agent"):
        self._core = core_v1
        self._ns = namespace
        self._selector = label_selector

    def list_replicas(self) -> list[RawReplica]:
        out: list[RawReplica] = []
        pods = self._core.list_namespaced_pod(
            namespace=self._ns,
            label_selector=self._selector,
        ).items
        for pod in pods:
            if pod.status.phase != "Running" or not pod.status.pod_ip:
                continue
            agent_name = (pod.metadata.labels or {}).get(self._selector, "unknown")
            port = next(
                (p.container_port for c in pod.spec.containers
                                       for p in (c.ports or [])
                                       if p.container_port == 5000),
                5000,
            )
            out.append(RawReplica(
                id=pod.metadata.uid or pod.metadata.name,
                container_name=pod.metadata.name,
                addr=f"http://{pod.status.pod_ip}:{port}",
                agent_name=agent_name,
            ))
        return out
