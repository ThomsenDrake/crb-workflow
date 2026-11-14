import math

from worker.workflows import odds_math as om


def test_american_to_decimal():
    assert om.american_to_decimal(-110) == 1.0 + 100.0 / 110.0
    assert om.american_to_decimal(150) == 2.5
    try:
        om.american_to_decimal(0)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_decimal_roundtrip():
    for a in (-5000, -800, -110, -105, 100, 250, 1800):
        assert om.decimal_to_american(om.american_to_decimal(a)) == a


def test_devig_two_way_sums_to_one():
    p1, p2 = om.devig_two_way(0.52381, 0.52381)
    assert math.isclose(p1, 0.5) and math.isclose(p2, 0.5)
    p1, p2 = om.devig_two_way(0.6, 0.45)
    assert math.isclose(p1 + p2, 1.0)
    assert p1 > 0.6 / 1.05  # devigging removes the vig


def test_cover_prob():
    # even game, home favored by 6.5 -> cover prob well below 0.5
    p = om.cover_prob(0.0, 14.0, -6.5)
    assert 0.0 < p < 0.5
    # huge favorite with big expected margin -> high cover prob (Phi(0.536) ~ 0.70)
    assert om.cover_prob(14.0, 14.0, -6.5) > 0.7


def test_over_prob():
    assert om.over_prob(50.0, 11.0, 44.5) > 0.5
    assert om.over_prob(40.0, 11.0, 44.5) < 0.5


def test_expected_value_and_kelly():
    assert om.expected_value(0.6, 2.0) == 0.6 * 1.0 - 0.4
    assert om.kelly_fraction(0.4, 2.0) == 0.0  # no edge
    f = om.kelly_fraction(0.6, 2.0)
    assert 0.0 < f < 1.0
    assert om.kelly_fraction(0.6, 1.0) == 0.0  # invalid odds
