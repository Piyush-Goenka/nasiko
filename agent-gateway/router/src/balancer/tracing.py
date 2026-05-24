"""
Tracing wiring for the balancer.

Reuses Nasiko's already-installed arize-phoenix-otel package when available.
Falls back to a noop tracer if Phoenix isn't reachable so the router stays up.
W3C traceparent + baggage are always propagated; downstream replicas see
trace context regardless of whether the exporter actually shipped a span.
"""

from opentelemetry import trace, propagate
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.baggage.propagation import W3CBaggagePropagator

_PROPAGATOR = CompositePropagator(
    [TraceContextTextMapPropagator(), W3CBaggagePropagator()]
)
propagate.set_global_textmap(_PROPAGATOR)


def setup_tracer(service_name: str = "nasiko-balancer",
                 endpoint: str | None = None,
                 enabled: bool = True):
    if not enabled:
        return trace.get_tracer(service_name)
    try:
        from phoenix.otel import register
        register(
            project_name=service_name,
            endpoint=endpoint or "http://phoenix-observability:4318/v1/traces",
            auto_instrument=True,
        )
    except Exception:
        # Phoenix not reachable at startup; keep router up, spans become no-ops.
        pass
    return trace.get_tracer(service_name)


def inject_traceparent(span, headers: dict) -> None:
    ctx = trace.set_span_in_context(span)
    _PROPAGATOR.inject(headers, context=ctx)


def start_route_span(pool: str, strategy: str, replica):
    tracer = trace.get_tracer("nasiko-balancer")
    span = tracer.start_span("lb.route")
    span.set_attribute("lb.pool", pool)
    span.set_attribute("lb.strategy", strategy)
    span.set_attribute("lb.selected_instance", replica.container_name)
    return span
