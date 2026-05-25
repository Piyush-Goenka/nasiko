import json
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from sse_starlette.sse import EventSourceResponse

from .events import EventBroker
from .metrics import REGISTRY, fairness_gini, gini
from .runtime import all_balancers, get_balancer_for


def build_router(registry, events: EventBroker) -> APIRouter:
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
            counts = [r.inflight for r in replicas]
            g = gini(counts)
            fairness_gini.labels(name).set(g)
            out.append({
                "agent_name": name,
                "strategy": lb.strategy_name,
                "healthy": sum(1 for r in replicas if r.status.value == "serving"),
                "total": len(replicas),
                "fairness_gini": g,
            })
        return {"pools": out}

    @router.put("/strategy/{agent_name}")
    def set_strategy(agent_name: str, body: dict):
        lb = get_balancer_for(agent_name)
        if lb is None:
            raise HTTPException(404, "no balancer")
        strategy = body.get("strategy")
        if not strategy:
            raise HTTPException(400, "missing strategy")
        try:
            lb.set_strategy(strategy)
        except ValueError as e:
            raise HTTPException(400, str(e))
        return {"agent_name": agent_name, "strategy": strategy}

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
