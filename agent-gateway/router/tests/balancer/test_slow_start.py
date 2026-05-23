import time
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.slow_start import SlowStart


def test_slow_start_weight_grows_linearly_over_window():
    now = time.monotonic()
    r = Replica(
        id="x",
        agent_name="t",
        container_name="x",
        addr="http://x:5000",
        status=ReplicaStatus.SERVING,
        joined_at=now - 15.0,
    )
    ss = SlowStart(window_seconds=30.0)
    w = ss.weight(r, now=now)
    assert 0.49 <= w <= 0.51


def test_slow_start_weight_caps_at_one():
    now = time.monotonic()
    r = Replica(
        id="x",
        agent_name="t",
        container_name="x",
        addr="http://x:5000",
        status=ReplicaStatus.SERVING,
        joined_at=now - 60.0,
    )
    ss = SlowStart(window_seconds=30.0)
    assert ss.weight(r, now=now) == 1.0


def test_slow_start_disabled_returns_one():
    now = time.monotonic()
    r = Replica(
        id="x",
        agent_name="t",
        container_name="x",
        addr="http://x:5000",
        status=ReplicaStatus.SERVING,
        joined_at=now,
    )
    ss = SlowStart(window_seconds=0.0)
    assert ss.weight(r, now=now) == 1.0
