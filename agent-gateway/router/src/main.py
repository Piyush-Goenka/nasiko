"""
Refactored main router application with modular architecture.
"""

import asyncio
import logging
import os
import time
from io import BytesIO
from typing import List, Optional

import httpx
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.params import Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from router.src.config import settings
from router.src.entities import UserRequest
from router.src.services import RouterOrchestrator

# Balancer wiring
from router.src.balancer.api import build_router as build_balancer_router
from router.src.balancer.circuit_breaker import CircuitBreaker
from router.src.balancer.discovery.docker_adapter import DockerDiscoveryAdapter
from router.src.balancer.discovery.k8s_adapter import K8sDiscoveryAdapter
from router.src.balancer.events import EventBroker
from router.src.balancer.health import HealthChecker
from router.src.balancer.load_balancer import LoadBalancer
from router.src.balancer.models import Event
from router.src.balancer.registry import InstanceRegistry
from router.src.balancer.runtime import (
    close_shared_http_client, register_passive_observer,
    set_balancer_for, setup_shared_http_client,
)
from router.src.balancer.slow_start import SlowStart
from router.src.balancer.strategies.round_robin import RoundRobin
from router.src.balancer.tracing import setup_tracer
from router.src.core.agent_client import AgentClientError

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Balancer-wide shared state
_balancer_events = EventBroker()
_balancer_breakers: dict[str, CircuitBreaker] = {}
_balancer_slow_start = SlowStart(
    window_seconds=float(os.environ.get("BALANCER_SLOW_START_WINDOW_S", "30")),
)
_balancer_registry: Optional[InstanceRegistry] = None
_balancer_health: Optional[HealthChecker] = None
_balancer_http: Optional[httpx.AsyncClient] = None

# Security
security = HTTPBearer()

# Initialize FastAPI app
app = FastAPI(
    title="Nasiko Router Service",
    description="AI-powered agent routing service",
    version="2.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize orchestrator
orchestrator = RouterOrchestrator()


def _build_discovery_adapter():
    """Pick Docker or Kubernetes discovery based on K8S_ENABLED (mirrors registry.py)."""
    k8s_enabled = os.environ.get("K8S_ENABLED", "true").strip().lower() in {
        "1", "true", "yes", "y", "on",
    }
    if k8s_enabled:
        try:
            from kubernetes import client as k8s_client, config as k8s_config
            try:
                k8s_config.load_incluster_config()
            except Exception:
                k8s_config.load_kube_config()
            core_v1 = k8s_client.CoreV1Api()
            namespace = os.environ.get("AGENTS_NAMESPACE", "nasiko-agents")
            logger.info(f"Balancer: using K8s discovery in namespace={namespace}")
            return K8sDiscoveryAdapter(core_v1=core_v1, namespace=namespace)
        except Exception as e:
            logger.warning(
                f"Balancer: K8s adapter unavailable ({e}); falling back to Docker"
            )
    try:
        import docker
        docker_client = docker.from_env()
        network = os.environ.get("AGENTS_NETWORK", "agents-net")
        logger.info(f"Balancer: using Docker discovery on network={network}")
        return DockerDiscoveryAdapter(docker_client=docker_client, network=network)
    except Exception as e:
        logger.warning(f"Balancer: no discovery adapter available ({e}); pool will be empty")
        return None


@app.on_event("startup")
async def _balancer_startup():
    global _balancer_registry, _balancer_health, _balancer_http

    setup_tracer(
        service_name="nasiko-balancer",
        endpoint=os.environ.get(
            "PHOENIX_OTLP_ENDPOINT",
            "http://phoenix-observability:4318/v1/traces",
        ),
        enabled=os.environ.get("BALANCER_TRACING", "true").lower() == "true",
    )

    # Pooled HTTP client for the agent-call hot path. Keeps connections warm
    # across requests; bounded to 100 concurrent sockets to protect against
    # runaway fan-out. Must live inside the event loop, so we create it here.
    setup_shared_http_client(
        timeout=httpx.Timeout(settings.REQUEST_TIMEOUT),
        max_connections=int(os.environ.get("BALANCER_HTTP_MAX_CONNECTIONS", "100")),
        max_keepalive_connections=int(
            os.environ.get("BALANCER_HTTP_MAX_KEEPALIVE", "20")
        ),
    )

    adapter = _build_discovery_adapter()
    if adapter is None:
        logger.warning("Balancer: discovery disabled; AgentClient will use legacy single-URL path")
        return

    _balancer_registry = InstanceRegistry(
        adapter=adapter,
        refresh_interval=float(os.environ.get("BALANCER_REFRESH_INTERVAL_S", "5")),
    )

    _balancer_http = httpx.AsyncClient(timeout=2.0)

    async def http_get(url, timeout):
        return await _balancer_http.get(url, timeout=timeout)

    _balancer_health = HealthChecker(
        breakers=_balancer_breakers,
        http_get=http_get,
        interval=float(os.environ.get("BALANCER_HEALTH_INTERVAL_S", "5")),
        timeout=float(os.environ.get("BALANCER_HEALTH_TIMEOUT_S", "2")),
    )

    seen_agents: set[str] = set()

    def on_change(kind: str, r):
        if kind == "added":
            _balancer_breakers[r.container_name] = CircuitBreaker(
                failure_threshold=int(os.environ.get("BALANCER_CB_FAILURE_THRESHOLD", "5")),
                cooldown_initial=float(os.environ.get("BALANCER_CB_COOLDOWN_INITIAL_S", "10")),
                cooldown_max=float(os.environ.get("BALANCER_CB_COOLDOWN_MAX_S", "300")),
            )
            _balancer_events.emit(Event(
                ts=time.time(), type="replica_added",
                pool=r.agent_name, instance_id=r.container_name,
                detail={"addr": r.addr},
            ))
            if r.agent_name not in seen_agents:
                seen_agents.add(r.agent_name)
                lb = LoadBalancer(
                    r.agent_name, _balancer_registry, RoundRobin(),
                    _balancer_breakers, _balancer_slow_start,
                )
                set_balancer_for(r.agent_name, lb)
            asyncio.create_task(_balancer_health.run_probe(r))
        elif kind == "removed":
            _balancer_events.emit(Event(
                ts=time.time(), type="replica_terminated",
                pool=r.agent_name, instance_id=r.container_name,
            ))

    _balancer_registry.subscribe(on_change)
    register_passive_observer(_balancer_health.passive_observe)
    await _balancer_registry.start()
    logger.info("Balancer: started")


@app.on_event("shutdown")
async def _balancer_shutdown():
    global _balancer_http
    if _balancer_registry is not None:
        await _balancer_registry.stop()
    if _balancer_http is not None:
        await _balancer_http.aclose()
        _balancer_http = None
    await close_shared_http_client()


class _LazyRegistryProxy:
    """The balancer router needs a registry, but the InstanceRegistry isn't
    constructed until the startup event runs. Proxy lookups so include_router
    can be wired at import time."""

    def replicas_for(self, agent_name: str):
        if _balancer_registry is None:
            return []
        return _balancer_registry.replicas_for(agent_name)

    def all_replicas(self):
        if _balancer_registry is None:
            return []
        return _balancer_registry.all_replicas()


app.include_router(build_balancer_router(
    registry=_LazyRegistryProxy(),
    events=_balancer_events,
))


@app.exception_handler(AgentClientError)
async def _agent_client_error(req: Request, exc: AgentClientError):
    """Convert pool-exhaustion errors into 503 + Retry-After instead of 500."""
    msg = str(exc)
    if "no healthy replicas" in msg:
        return JSONResponse(
            status_code=503,
            content={"error": msg, "retry_after_seconds": 5},
            headers={"Retry-After": "5"},
        )
    return JSONResponse(status_code=502, content={"error": msg})


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        health_status = await orchestrator.health_check()
        return health_status
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@app.get("/router/health")
async def health():
    return {"status": "ok"}


@app.post("/router")
async def process_request(
    session_id: str = Form(...),
    query: str = Form(...),
    route: Optional[str] = Form(None),
    files: Optional[List[UploadFile]] = File(
        None,
        max_length=settings.MAX_FILE_SIZE,
        description="Optional files to upload (PDF, TXT, DOCX, XLSX)",
    ),
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> StreamingResponse:
    """
    Process a user request through the router pipeline.

    Args:
        session_id: Unique session identifier
        query: User query text
        route: Optional direct route to specific agent
        files: Optional files to upload
        credentials: Bearer token credentials

    Returns:
        Streaming response with router processing updates

    Raises:
        HTTPException: For validation or processing errors
    """
    try:
        # Validate inputs
        validation_error = _validate_inputs(session_id, query)
        if validation_error:
            logger.error(f"Validation error: {validation_error}")
            raise HTTPException(status_code=400, detail=validation_error)

        # Process files
        files_to_forward = await _process_files(files)

        # Create request object
        request = UserRequest(session_id=session_id, query=query, route=route)
        logger.info(f"Processing request: {request}")
        logger.info(f"Files count: {len(files_to_forward)}")

        # Extract token
        token = credentials.credentials

        # Process through orchestrator
        return StreamingResponse(
            orchestrator.process_request(request, files_to_forward, token),
            media_type="application/json",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in process endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/metrics")
async def get_metrics():
    """Get router service metrics."""
    # TODO: Implement metrics collection
    return {
        "requests_processed": 0,
        "active_sessions": 0,
        "average_response_time": 0.0,
        "error_rate": 0.0,
    }


def _validate_inputs(session_id: str, query: str) -> Optional[str]:
    """
    Validate request inputs.

    Args:
        session_id: Session identifier
        query: User query

    Returns:
        Error message if validation fails, None otherwise
    """
    if not session_id or not session_id.strip():
        return "session_id cannot be empty"
    logger.info(f"Session id: {session_id}")

    if not query or not query.strip():
        return "query cannot be empty"

    return None


async def _process_files(files: Optional[List[UploadFile]]) -> List[tuple]:
    """
    Process uploaded files for forwarding.

    Args:
        files: List of uploaded files

    Returns:
        List of file tuples ready for forwarding

    Raises:
        HTTPException: If file processing fails
    """
    files_to_forward = []

    if not files:
        return files_to_forward

    for file in files:
        try:
            if file.size and file.size > settings.MAX_FILE_SIZE:
                raise HTTPException(
                    status_code=413,
                    detail=f"File {file.filename} exceeds maximum size of {settings.MAX_FILE_SIZE} bytes",
                )

            content_bytes = await file.read()
            bio = BytesIO(content_bytes)
            bio.seek(0)

            files_to_forward.append(
                (
                    "files",
                    (
                        file.filename,
                        bio,
                        file.content_type or "application/octet-stream",
                    ),
                )
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error reading file {file.filename}: {e}")
            raise HTTPException(
                status_code=400, detail=f"Failed to read file {file.filename}: {str(e)}"
            )

    return files_to_forward


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
        log_level=settings.LOG_LEVEL.lower(),
    )
