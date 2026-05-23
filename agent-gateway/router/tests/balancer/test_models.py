import time
from router.src.balancer.models import Replica, ReplicaStatus, Event

def test_replica_defaults():
    r = Replica(
        id="abc123", agent_name="translator",
        container_name="a2a-translator-2",
        addr="http://a2a-translator-2:5000",
        status=ReplicaStatus.DISCOVERED,
        joined_at=time.monotonic(),
    )
    assert r.inflight == 0
    assert r.consecutive_5xx == 0
    assert r.ewma_latency_ms == 0.0
    assert r.status is ReplicaStatus.DISCOVERED

def test_event_serializes_to_dict():
    e = Event(ts=1.0, type="replica_added", pool="translator",
              instance_id="abc123", detail={"addr": "http://x:5000"})
    d = e.to_dict()
    assert d["type"] == "replica_added"
    assert d["detail"]["addr"] == "http://x:5000"
