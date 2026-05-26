import random
from router.src.balancer.metrics import (
    RequestCounter, coefficient_of_variation, gini, status_class,
)


def test_gini_zero_for_equal_distribution():
    assert gini([10, 10, 10, 10]) == 0.0


def test_gini_one_for_total_concentration():
    g = gini([100, 0, 0, 0])
    assert 0.7 < g < 0.8  # exactly 0.75 for N=4, all to one


def test_gini_handles_empty_and_all_zero():
    assert gini([]) == 0.0
    assert gini([0, 0, 0]) == 0.0


def test_cv_zero_for_equal():
    assert coefficient_of_variation([5, 5, 5]) == 0.0


def test_cv_positive_for_uneven():
    assert coefficient_of_variation([1, 10]) > 0.5


def test_status_class_buckets():
    assert status_class(200) == "2xx"
    assert status_class(301) == "3xx"
    assert status_class(404) == "4xx"
    assert status_class(503) == "5xx"


def _naive_gini(values: list[float]) -> float:
    """The original O(N^2) double-sum implementation, kept for parity testing."""
    if not values:
        return 0.0
    n = len(values)
    s = sum(values)
    if s == 0:
        return 0.0
    diffs = sum(abs(a - b) for a in values for b in values)
    return diffs / (2 * n * n * (s / n))


def test_gini_closed_form_matches_naive_on_random_inputs():
    """Regression for T8: closed-form O(N log N) Gini must agree with the
    naive O(N^2) double-sum to floating-point tolerance."""
    rng = random.Random(42)
    for _ in range(100):
        n = rng.randint(2, 50)
        values = [rng.uniform(0, 1000) for _ in range(n)]
        assert abs(gini(values) - _naive_gini(values)) < 1e-9


def test_request_counter_rolling_window_drops_old_events():
    """Regression for T3 (storage layer): events older than the window must
    not contribute to the count, so a quiet period naturally decays Gini."""
    rc = RequestCounter(window_seconds=60.0)
    rc.record("translator", "a", now=0.0)
    rc.record("translator", "a", now=10.0)
    rc.record("translator", "b", now=20.0)
    # Read at t=30: all three events within the 60s window.
    assert rc.counts_for("translator", ["a", "b"], now=30.0) == [2, 1]
    # Read at t=120: only the t=20 + t=60 cutoff line survives; a's events
    # at t=0 and t=10 fall out (older than 60s).
    assert rc.counts_for("translator", ["a", "b"], now=120.0) == [0, 0]


def test_request_counter_gini_reports_imbalance_then_decays():
    """Regression for T3 (end-to-end): a 90/5/5 split should produce
    Gini ~ 0.6 (clear imbalance), and after the window expires Gini should
    fall back to 0 because every count is 0."""
    rc = RequestCounter(window_seconds=60.0)
    now = 1000.0
    for _ in range(90):
        rc.record("translator", "a", now=now)
    for _ in range(5):
        rc.record("translator", "b", now=now)
    for _ in range(5):
        rc.record("translator", "c", now=now)
    counts = rc.counts_for("translator", ["a", "b", "c"], now=now)
    assert counts == [90, 5, 5]
    g_busy = gini(counts)
    assert 0.5 < g_busy < 0.7, f"expected Gini ~0.57 for 90/5/5, got {g_busy:.3f}"
    # After the window expires, all counts revert to 0 -> Gini collapses to 0.
    later_counts = rc.counts_for("translator", ["a", "b", "c"], now=now + 120)
    assert later_counts == [0, 0, 0]
    assert gini(later_counts) == 0.0
