import re
from .base import RawReplica

_DENYLIST = {
    "kong-gateway", "kong-database", "redis", "mongodb",
    "phoenix-observability", "nasiko-router", "nasiko-web",
    "nasiko-backend", "nasiko-auth-service", "chat-history-service",
    "kong-service-registry", "kong-migrations",
    "nasiko-superuser-init", "nasiko-redis-listener",
}
_NAME_RE = re.compile(r"^(?:agent-|a2a-)?([a-z0-9_-]+?)(?:-\d+)?$")
_PREFIXES = ("agent-", "a2a-")


class DockerDiscoveryAdapter:
    def __init__(self, docker_client, network: str = "agents-net"):
        self._docker = docker_client
        self._network = network

    def list_replicas(self) -> list[RawReplica]:
        out: list[RawReplica] = []
        network = self._docker.networks.get(self._network)
        for c in network.containers:
            if c.name in _DENYLIST or not c.name.startswith(_PREFIXES):
                continue
            label = (c.labels or {}).get("com.nasiko.agent")
            if label:
                agent_name = label
            else:
                m = _NAME_RE.match(c.name)
                agent_name = m.group(1) if m else c.name
            out.append(RawReplica(
                id=c.short_id,
                container_name=c.name,
                addr=f"http://{c.name}:5000",
                agent_name=agent_name,
            ))
        return out
