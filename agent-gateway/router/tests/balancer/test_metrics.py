from router.src.balancer.metrics import gini, coefficient_of_variation, status_class


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
