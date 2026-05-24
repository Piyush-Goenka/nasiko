from router.src.balancer.tracing import setup_tracer, inject_traceparent


def test_setup_tracer_returns_a_tracer():
    tracer = setup_tracer(
        service_name="nasiko-balancer",
        endpoint="http://phoenix-observability:4318/v1/traces",
        enabled=False,  # disabled in unit test (no Phoenix container)
    )
    assert tracer is not None


def test_inject_traceparent_does_not_raise():
    tracer = setup_tracer(service_name="t", endpoint="", enabled=False)
    span = tracer.start_span("lb.route")
    headers: dict = {}
    inject_traceparent(span, headers)
    span.end()
    # When tracer is a noop (test environment), headers may stay empty.
    # The contract is "doesn't raise"; actual propagation is verified in integration.
    assert isinstance(headers, dict)
