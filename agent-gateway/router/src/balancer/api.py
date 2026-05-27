import json
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from .events import EventBroker
from .hedging import LatencyTracker
from .metrics import REGISTRY, fairness_gini, gini, publish_pool_gauges, request_counter
from .models import Event
from .runtime import all_balancers, get_balancer_for


class StrategyUpdate(BaseModel):
    """Request body for PUT /balancer/strategy/{agent_name}.

    `strategy` is required and validated against the registered strategy
    names. `hedging` is optional; passing it toggles per-pool hedging at
    runtime without restarting the router.
    """

    strategy: str = Field(..., min_length=1)
    hedging: Optional[bool] = None


def build_router(registry, events: EventBroker, breakers: dict | None = None) -> APIRouter:
    """
    FastAPI router exposing the balancer dashboard surface.

    Endpoints:
      GET  /balancer/pool/{agent_name}     snapshot of a single agent's pool
      GET  /balancer/pools                 summary of all pools (+ fairness Gini)
      PUT  /balancer/strategy/{agent_name} hot-swap the active strategy
      GET  /balancer/events                SSE stream of pool lifecycle events
      GET  /balancer/metrics               Prometheus exposition
    """

    router = APIRouter(prefix="/balancer", tags=["balancer"])

    @router.get("/pool/{agent_name}")
    def get_pool(agent_name: str):
        lb = get_balancer_for(agent_name)
        if lb is None:
            raise HTTPException(404, "no balancer for agent")
        replicas = registry.replicas_for(agent_name)
        return {
            "agent_name": agent_name,
            "strategy": lb.strategy_name,
            "replicas": [
                {
                    "id": r.id,
                    "container_name": r.container_name,
                    "addr": r.addr,
                    "status": r.status.value,
                    "inflight": r.inflight,
                    "ewma_latency_ms": r.ewma_latency_ms,
                    "consecutive_5xx": r.consecutive_5xx,
                }
                for r in replicas
            ],
        }

    @router.get("/pools")
    def get_pools():
        out = []
        for name, lb in all_balancers().items():
            replicas = registry.replicas_for(name)
            # Use a 60-second rolling request count, not instantaneous inflight,
            # so the fairness number reflects historical distribution instead of
            # collapsing to 0 between bursts.
            counts = request_counter.counts_for(
                name, [r.container_name for r in replicas]
            )
            g = gini(counts)
            fairness_gini.labels(name).set(g)
            if breakers is not None:
                publish_pool_gauges(name, replicas, breakers, lb.strategy_name)
            out.append({
                "agent_name": name,
                "strategy": lb.strategy_name,
                "healthy": sum(1 for r in replicas if r.status.value == "serving"),
                "total": len(replicas),
                "fairness_gini": g,
            })
        return {"pools": out}

    @router.put("/strategy/{agent_name}")
    def set_strategy(agent_name: str, body: StrategyUpdate):
        lb = get_balancer_for(agent_name)
        if lb is None:
            raise HTTPException(404, "no balancer")
        previous_strategy = lb.strategy_name
        try:
            lb.set_strategy(body.strategy)
        except ValueError as e:
            raise HTTPException(400, str(e))
        hedging_changed = False
        if body.hedging is not None:
            previous_hedging = lb.hedging_enabled
            lb.hedging_enabled = body.hedging
            # Lazy-init a tracker so hedging can be toggled on without a
            # restart. Existing tracker is kept (we want its accumulated
            # samples) to avoid resetting the p95 trigger.
            if body.hedging and lb.latency_tracker is None:
                lb.latency_tracker = LatencyTracker()
            hedging_changed = previous_hedging != lb.hedging_enabled
        # Push the strategy gauge right away so the dashboard reflects the
        # swap without waiting for the next /pools poll.
        if breakers is not None:
            publish_pool_gauges(
                agent_name, registry.replicas_for(agent_name), breakers, body.strategy,
            )
        if previous_strategy != body.strategy or hedging_changed:
            events.emit(Event(
                ts=time.time(), type="strategy_changed",
                pool=agent_name, instance_id="",
                detail={
                    "from": previous_strategy,
                    "to": body.strategy,
                    "hedging": lb.hedging_enabled,
                },
            ))
        return {
            "agent_name": agent_name,
            "strategy": body.strategy,
            "hedging": lb.hedging_enabled,
        }

    @router.get("/events")
    async def stream_events():
        q = events.subscribe(replay=True)

        async def gen():
            try:
                while True:
                    e = await q.get()
                    yield {"event": e.type, "data": json.dumps(e.to_dict())}
            finally:
                events.unsubscribe(q)

        return EventSourceResponse(gen())

    @router.get("/metrics")
    def prometheus():
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    @router.get("/dashboard", include_in_schema=False)
    def dashboard():
        """Standalone single-file dashboard. Opens in any browser; uses the
        same balancer API (no build step)."""
        path = Path(__file__).parent / "dashboard.html"
        if not path.exists():
            raise HTTPException(404, "dashboard.html not bundled")
        return FileResponse(path, media_type="text/html")

    return router
