import time
from dataclasses import dataclass
from .models import Replica


@dataclass
class SlowStart:
    window_seconds: float = 30.0

    def weight(self, replica: Replica, now: float | None = None) -> float:
        if self.window_seconds <= 0:
            return 1.0
        elapsed = (now or time.monotonic()) - replica.joined_at
        return min(1.0, max(0.0, elapsed / self.window_seconds))
