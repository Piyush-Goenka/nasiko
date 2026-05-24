import asyncio
from collections import deque
from .models import Event


class EventBroker:
    """
    Ring-buffered lifecycle event broker.

    Holds the last `max_history` events for replay-on-subscribe.
    Subscribers each get their own asyncio.Queue; emit() broadcasts.
    Slow subscribers do not block emitters: full queues drop new events.
    """

    def __init__(self, max_history: int = 1024):
        self._history: deque[Event] = deque(maxlen=max_history)
        self._subscribers: list[asyncio.Queue[Event]] = []

    def emit(self, event: Event) -> None:
        self._history.append(event)
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def history(self) -> list[Event]:
        return list(self._history)

    def subscribe(self, replay: bool = False, maxsize: int = 256) -> asyncio.Queue[Event]:
        q: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        if replay:
            for e in self._history:
                try:
                    q.put_nowait(e)
                except asyncio.QueueFull:
                    break
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[Event]) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass
