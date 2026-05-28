"""
Tracing wiring for the balancer.

Reuses Nasiko's already-installed arize-phoenix-otel package when available.
Falls back to a noop tracer if Phoenix isn't reachable so the router stays up.
W3C traceparent + baggage are always propagated; downstream replicas see
trace context regardless of whether the exporter actually shipped a span.
"""

from opentelemetry import trace, propagate
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.trace import NoOpTracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.baggage.propagation import W3CBaggagePropagator

_PROPAGATOR = CompositePropagator(
    [TraceContextTextMapPropagator(), W3CBaggagePropagator()]
)
propagate.set_global_textmap(_PROPAGATOR)

# Cached tracer chosen at startup. Holding a reference avoids a global lookup
# on every routing decision and, more importantly, lets us return a real
# NoOpTracer when tracing is disabled instead of relying on the OTel default
# tracer's implicit no-op-when-no-provider behaviour, which can still attempt
# OTLP exports under partial-init conditions.
_TRACER = NoOpTracer()


def setup_tracer(service_name: str = "nasiko-balancer",
                 endpoint: str | None = None,
                 enabled: bool = True):
    global _TRACER
    if not enabled:
        _TRACER = NoOpTracer()
        return _TRACER
    try:
        from phoenix.otel import register
        register(
            project_name=service_name,
            endpoint=endpoint or "http://phoenix-observability:4318/v1/traces",
            auto_instrument=True,
        )
        _TRACER = trace.get_tracer(service_name)
    except Exception:
        # Phoenix not reachable at startup; keep router up, spans become no-ops.
        _TRACER = NoOpTracer()
    return _TRACER


def inject_traceparent(span, headers: dict) -> None:
    ctx = trace.set_span_in_context(span)
    _PROPAGATOR.inject(headers, context=ctx)


def start_route_span(
    pool: str,
    strategy: str,
    replica,
    *,
    pool_size: int | None = None,
    healthy_count: int | None = None,
    decision_latency_us: float | None = None,
    candidates_considered: int | None = None,
    inflight_at_selection: int | None = None,
):
    """
    Start the `lb.route` span. The implicit current OTel context is used as
    the parent automatically, so this span chains under any upstream span
    Nasiko set up (router, orchestrator). Optional kwargs fill in the
    routing-decision attributes the plan calls out; passing None for any
    of them skips that attribute rather than poisoning the trace with
    sentinel values.
    """
    span = _TRACER.start_span("lb.route")
    span.set_attribute("lb.pool", pool)
    span.set_attribute("lb.strategy", strategy)
    span.set_attribute("lb.selected_instance", replica.container_name)
    if pool_size is not None:
        span.set_attribute("lb.pool_size", pool_size)
    if healthy_count is not None:
        span.set_attribute("lb.healthy_count", healthy_count)
    if decision_latency_us is not None:
        span.set_attribute("lb.decision_latency_us", float(decision_latency_us))
    if candidates_considered is not None:
        span.set_attribute("lb.candidates_considered", candidates_considered)
    if inflight_at_selection is not None:
        span.set_attribute("lb.inflight_at_selection", inflight_at_selection)
    return span
