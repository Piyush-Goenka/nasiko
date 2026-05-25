import asyncio
import pytest
from router.src.balancer.hedging import LatencyTracker, hedge_request


def test_latency_tracker_p95_after_warmup():
    lt = LatencyTracker(window=100)
    for ms in [10] * 95 + [500] * 5:
        lt.observe(ms / 1000)
    p95 = lt.p95_seconds()
    assert 0.4 <= p95 <= 0.6


def test_latency_tracker_warmup_returns_safe_default():
    lt = LatencyTracker(window=100)
    # Below warmup threshold (20 samples), should return safe default
    for _ in range(5):
        lt.observe(0.01)
    assert lt.p95_seconds() == 1.0


@pytest.mark.asyncio
async def test_hedge_fires_after_p95_and_returns_first_winner():
    async def slow():
        await asyncio.sleep(0.5)
        return "slow"

    async def fast():
        await asyncio.sleep(0.05)
        return "fast"

    winner = await hedge_request(primary=slow, secondary=fast, hedge_after_s=0.1)
    assert winner == "fast"


@pytest.mark.asyncio
async def test_hedge_returns_primary_if_it_finishes_first():
    async def quick():
        await asyncio.sleep(0.01)
        return "primary"

    async def never():
        await asyncio.sleep(10)
        return "secondary"

    winner = await hedge_request(primary=quick, secondary=never, hedge_after_s=0.5)
    assert winner == "primary"
