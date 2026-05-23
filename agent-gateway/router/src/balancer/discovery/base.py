from dataclasses import dataclass
from typing import Protocol


@dataclass
class RawReplica:
    """
    Minimal replica payload returned by a DiscoveryAdapter, before the
    InstanceRegistry assigns lifecycle state. Container_name is the
    Docker container name or the Kubernetes pod name.
    """

    id: str
    container_name: str
    addr: str
    agent_name: str


class DiscoveryAdapter(Protocol):
    def list_replicas(self) -> list[RawReplica]: ...
