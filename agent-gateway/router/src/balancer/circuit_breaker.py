import time
from dataclasses import dataclass, field
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """
    Per-replica circuit breaker with Hystrix-style single-probe half-open recovery.

    Transitions:
      CLOSED  ---failure_threshold consecutive failures---> OPEN
      OPEN    ---cooldown elapsed--------------------------> HALF_OPEN
      HALF_OPEN ---probe succeeds-------------------------> CLOSED
      HALF_OPEN ---probe fails----------------------------> OPEN (cooldown *= 2, capped)
    """

    failure_threshold: int = 5
    cooldown_initial: float = 10.0
    cooldown_max: float = 300.0
    state: CircuitState = CircuitState.CLOSED
    _consecutive_failures: int = 0
    _opened_at: float = 0.0
    _current_cooldown: float = field(init=False)
    _half_open_probe_admitted: bool = False

    def __post_init__(self) -> None:
        self._current_cooldown = self.cooldown_initial

    def on_success(self) -> None:
        if self.state is CircuitState.HALF_OPEN:
            self.state = CircuitState.CLOSED
            self._current_cooldown = self.cooldown_initial
        self._consecutive_failures = 0
        self._half_open_probe_admitted = False

    def on_failure(self) -> None:
        if self.state is CircuitState.HALF_OPEN:
            self._open(double_cooldown=True)
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.failure_threshold:
            self._open(double_cooldown=False)

    def _open(self, double_cooldown: bool) -> None:
        if double_cooldown and self.state is CircuitState.HALF_OPEN:
            self._current_cooldown = min(self._current_cooldown * 2, self.cooldown_max)
        self.state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        self._half_open_probe_admitted = False

    def can_pass(self) -> bool:
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self._current_cooldown:
                self.state = CircuitState.HALF_OPEN
                self._half_open_probe_admitted = False
            else:
                return False
        if self.state is CircuitState.HALF_OPEN:
            if not self._half_open_probe_admitted:
                self._half_open_probe_admitted = True
                return True
            return False
        return False
