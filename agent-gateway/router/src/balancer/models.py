from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any

class ReplicaStatus(str, Enum):
    DISCOVERED = "discovered"
    READY = "ready"
    SERVING = "serving"
    DRAINING = "draining"
    EJECTED = "ejected"
    TERMINATED = "terminated"

@dataclass
class Replica:
    id: str
    agent_name: str
    container_name: str
    addr: str
    status: ReplicaStatus
    joined_at: float
    inflight: int = 0
    consecutive_health_failures: int = 0
    consecutive_5xx: int = 0
    ewma_latency_ms: float = 0.0

@dataclass(frozen=True)
class Event:
    """Immutable lifecycle record. Frozen because events are append-only:
    once emitted, they must not be mutated by subscribers (the same
    instance is replayed to every SSE consumer)."""
    ts: float
    type: str
    pool: str
    instance_id: str
    detail: dict[str, Any] = field(default_factory=dict)
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
