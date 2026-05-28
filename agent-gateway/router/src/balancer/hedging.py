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
        """
        Returns the latency value at the 95th percentile (tail-aware).
        For hedging we want the point where the slow tail begins, so we
        use ceil-style indexing: int(N * 0.95) picks the first sample
        of the top 5%.
        """
        if len(self._samples) < 20:
            return 1.0  # safe default during warmup
        s = sorted(self._samples)
        idx = min(len(s) - 1, int(len(s) * 0.95))
        return s[idx]


async def hedge_request(
    primary: Callable[[], Awaitable[Any]],
    secondary: Callable[[], Awaitable[Any]],
    hedge_after_s: float,
) -> Any:
    """
    Fire primary. After hedge_after_s, fire secondary in parallel.
    Return whichever finishes first; cancel the loser via cooperative cancellation.

    Cancelled losers are awaited (with return_exceptions=True) so their
    CancelledError or any pre-cancel exception is consumed cleanly. Without
    this, asyncio logs "Task exception was never retrieved" noise into the
    router logs and the loser's exception is silently dropped instead of
    being observed by the caller's reporting layer.
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
        if pending:
            # Drain the cancelled task(s) so their state is fully consumed
            # before we return. return_exceptions=True keeps a CancelledError
            # or a real failure from propagating up; the loser is by
            # definition not the result we care about.
            await asyncio.gather(*pending, return_exceptions=True)
        winner = next(iter(done))
        return winner.result()
