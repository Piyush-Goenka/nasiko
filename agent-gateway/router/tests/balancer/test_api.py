import time
from fastapi import FastAPI
from fastapi.testclient import TestClient
from router.src.balancer.api import build_router
from router.src.balancer.circuit_breaker import CircuitBreaker
from router.src.balancer.events import EventBroker
from router.src.balancer.load_balancer import LoadBalancer
from router.src.balancer.models import Replica, ReplicaStatus
from router.src.balancer.runtime import clear_balancers, set_balancer_for
from router.src.balancer.slow_start import SlowStart
from router.src.balancer.strategies.round_robin import RoundRobin


class _Reg:
    def __init__(self, r):
        self.r = r

    def replicas_for(self, _):
        return self.r

    def all_replicas(self):
        return self.r


def _setup():
    clear_balancers()
    a = Replica(id="a", agent_name="t", container_name="a",
                addr="http://a:5000", status=ReplicaStatus.SERVING,
                joined_at=time.monotonic())
    cbs = {"a": CircuitBreaker()}
    lb = LoadBalancer("t", _Reg([a]), RoundRobin(), cbs,
                      slow_start=SlowStart(window_seconds=0))
    set_balancer_for("t", lb)
    return _Reg([a]), EventBroker()


def test_get_pool_returns_snapshot():
    reg, broker = _setup()
    app = FastAPI()
    app.include_router(build_router(registry=reg, events=broker))
    client = TestClient(app)
    resp = client.get("/balancer/pool/t")
    assert resp.status_code == 200
    body = resp.json()
    assert body["agent_name"] == "t"
    assert len(body["replicas"]) == 1
    assert body["strategy"] == "round_robin"


def test_put_strategy_swaps_live():
    reg, broker = _setup()
    app = FastAPI()
    app.include_router(build_router(registry=reg, events=broker))
    client = TestClient(app)
    r = client.put("/balancer/strategy/t", json={"strategy": "random"})
    assert r.status_code == 200
    assert client.get("/balancer/pool/t").json()["strategy"] == "random"


def test_get_pools_summary():
    reg, broker = _setup()
    app = FastAPI()
    app.include_router(build_router(registry=reg, events=broker))
    client = TestClient(app)
    r = client.get("/balancer/pools")
    assert r.status_code == 200
    assert "t" in {p["agent_name"] for p in r.json()["pools"]}


def test_unknown_strategy_returns_400():
    reg, broker = _setup()
    app = FastAPI()
    app.include_router(build_router(registry=reg, events=broker))
    client = TestClient(app)
    r = client.put("/balancer/strategy/t", json={"strategy": "made-up-name"})
    assert r.status_code == 400


def test_metrics_endpoint_returns_prom_text():
    reg, broker = _setup()
    app = FastAPI()
    app.include_router(build_router(registry=reg, events=broker))
    client = TestClient(app)
    r = client.get("/balancer/metrics")
    assert r.status_code == 200
    assert b"lb_requests_total" in r.content
