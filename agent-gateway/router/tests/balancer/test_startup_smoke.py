import os
import pytest


@pytest.mark.skipif(
    os.environ.get("CI") == "true" or os.environ.get("SKIP_STARTUP_SMOKE") == "true",
    reason="needs Docker socket / Phoenix endpoint",
)
def test_main_module_imports_with_balancer_wiring():
    """
    Smoke test: importing main.py must succeed and the balancer router must
    be mounted. Live container discovery and Phoenix exporter init are
    expected to no-op gracefully when their backends aren't reachable.
    """
    os.environ.setdefault("BALANCER_TRACING", "false")
    os.environ.setdefault("K8S_ENABLED", "false")
    # Dummy key so VectorStore init doesn't reject; we never actually call OpenAI.
    os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")
    from router.src.main import app

    paths = {route.path for route in app.routes}
    assert any(p.startswith("/balancer") for p in paths), (
        f"balancer endpoints not mounted; got {sorted(paths)[:20]}"
    )
