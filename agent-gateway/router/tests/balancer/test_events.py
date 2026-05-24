import asyncio
import pytest
from router.src.balancer.events import EventBroker
from router.src.balancer.models import Event


def test_ring_buffer_keeps_last_n():
    broker = EventBroker(max_history=3)
    for i in range(5):
        broker.emit(Event(ts=float(i), type="x", pool="p", instance_id=str(i)))
    history = broker.history()
    assert [e.instance_id for e in history] == ["2", "3", "4"]


@pytest.mark.asyncio
async def test_subscriber_receives_emitted_events():
    broker = EventBroker(max_history=10)
    q = broker.subscribe()
    broker.emit(Event(ts=1.0, type="x", pool="p", instance_id="a"))
    received = await asyncio.wait_for(q.get(), timeout=0.1)
    assert received.instance_id == "a"


@pytest.mark.asyncio
async def test_subscriber_replays_history_on_subscribe():
    broker = EventBroker(max_history=10)
    broker.emit(Event(ts=1.0, type="x", pool="p", instance_id="a"))
    broker.emit(Event(ts=2.0, type="x", pool="p", instance_id="b"))
    q = broker.subscribe(replay=True)
    e1 = await asyncio.wait_for(q.get(), timeout=0.1)
    e2 = await asyncio.wait_for(q.get(), timeout=0.1)
    assert (e1.instance_id, e2.instance_id) == ("a", "b")
