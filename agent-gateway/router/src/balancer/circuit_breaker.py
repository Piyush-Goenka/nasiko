import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional


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

    An optional `on_transition` callback fires whenever `state` actually
    changes (CLOSED->OPEN, OPEN->HALF_OPEN, HALF_OPEN->CLOSED,
    HALF_OPEN->OPEN). Wired by main.py to translate breaker transitions
    into circuit_open / circuit_half_open / circuit_closed SSE events.
    """

    failure_threshold: int = 5
    cooldown_initial: float = 10.0
    cooldown_max: float = 300.0
    state: CircuitState = CircuitState.CLOSED
    on_transition: Optional[Callable[[CircuitState], None]] = None
    _consecutive_failures: int = 0
    _opened_at: float = 0.0
    _current_cooldown: float = field(init=False)
    _half_open_probe_admitted: bool = False

    def __post_init__(self) -> None:
        self._current_cooldown = self.cooldown_initial

    def _set_state(self, new_state: CircuitState) -> None:
        if self.state is new_state:
            return
        self.state = new_state
        if self.on_transition is not None:
            try:
                self.on_transition(new_state)
            except Exception:
                # Observer errors must never break the breaker itself.
                pass

    def on_success(self) -> None:
        if self.state is CircuitState.HALF_OPEN:
            self._set_state(CircuitState.CLOSED)
            self._current_cooldown = self.cooldown_initial
        self._consecutive_failures = 0
        self._half_open_probe_admitted = False

    def on_failure(self) -> None:
        if self.state is CircuitState.HALF_OPEN:
            self._open()
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.failure_threshold:
            self._open()

    def _open(self) -> None:
        # Re-opening from HALF_OPEN means the probe failed: extend the
        # cooldown geometrically (capped) so a flapping replica gets
        # increasingly long timeouts. Opening from CLOSED keeps the
        # current cooldown unchanged (typically the initial value).
        if self.state is CircuitState.HALF_OPEN:
            self._current_cooldown = min(self._current_cooldown * 2, self.cooldown_max)
        self._set_state(CircuitState.OPEN)
        self._opened_at = time.monotonic()
        self._half_open_probe_admitted = False

    def can_pass(self) -> bool:
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self._current_cooldown:
                self._set_state(CircuitState.HALF_OPEN)
                self._half_open_probe_admitted = False
            else:
                return False
        if self.state is CircuitState.HALF_OPEN:
            if not self._half_open_probe_admitted:
                self._half_open_probe_admitted = True
                return True
            return False
        return False
