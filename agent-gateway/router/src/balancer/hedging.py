"""
Hedged requests at p95 (Dean & Barroso, "The Tail at Scale", CACM 2013).

If the primary call hasn't returned within hedge_after_s, fire the secondary
in parallel. First response wins, the loser is cancelled. Bounded extra load
(~5% at the p95 trigger) cuts p99.9 dramatically.

ONLY use for idempotent operations. For mutating tool calls (e.g.,
github-agent create-PR), hedging is unsafe and must be off.
"""

import asyncio
from collections import deque
from typing import Any, Awaitable, Callable


class LatencyTracker:
    """Per-pool rolling histogram for the p95 hedge trigger."""

    def __init__(self, window: int = 200):
        self._samples: deque[float] = deque(maxlen=window)

    def observe(self, seconds: float) -> None:
        self._samples.append(seconds)

    def p95_seconds(self) -> float:
        if len(self._samples) < 20:
            return 1.0  # safe default during warmup
        s = sorted(self._samples)
        idx = max(0, int(len(s) * 0.95) - 1)
        return s[idx]


async def hedge_request(
    primary: Callable[[], Awaitable[Any]],
    secondary: Callable[[], Awaitable[Any]],
    hedge_after_s: float,
) -> Any:
    """
    Fire primary. After hedge_after_s, fire secondary in parallel.
    Return whichever finishes first; cancel the loser via cooperative cancellation.
    """
    p_task = asyncio.create_task(primary())
    try:
        return await asyncio.wait_for(asyncio.shield(p_task), timeout=hedge_after_s)
    except asyncio.TimeoutError:
        s_task = asyncio.create_task(secondary())
        done, pending = await asyncio.wait(
            {p_task, s_task}, return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        winner = next(iter(done))
        return winner.result()
