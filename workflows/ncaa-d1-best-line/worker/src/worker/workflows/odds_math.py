"""Pure odds math used by the workflow activities.

American/decimal conversion, implied probabilities, vig removal (power method),
normal-approximation cover/over probabilities, expected value, and Kelly sizing.
No I/O in here, so every function is trivially testable.
"""
from __future__ import annotations

import math


def american_to_decimal(american: int) -> float:
    """Convert American odds to decimal odds. -110 -> 1.9091, +150 -> 2.5."""
    if american == 0:
        raise ValueError("American odds of 0 are invalid")
    if american > 0:
        return 1.0 + american / 100.0
    return 1.0 + 100.0 / abs(american)


def decimal_to_american(decimal: float) -> int:
    """Convert decimal odds back to American odds."""
    if decimal <= 1.0:
        raise ValueError("Decimal odds must be > 1")
    if decimal >= 2.0:
        return int(round((decimal - 1.0) * 100.0))
    return int(round(-100.0 / (decimal - 1.0)))


def implied_prob(decimal: float) -> float:
    """Raw implied probability (still includes the bookmaker's vig)."""
    return 1.0 / decimal


def devig_two_way(p1: float, p2: float, exponent: float = 2.0) -> tuple[float, float]:
    """Remove vig from a two-way market with the power method (default k=2).

    k=2 matches the empirically observed shape of bookmaker vig on main lines.
    Returns the fair (no-vig) probabilities for side 1 and side 2.
    """
    if not (0.0 < p1 < 1.0 and 0.0 < p2 < 1.0):
        raise ValueError("Implied probabilities must lie in (0, 1)")
    z = p1 ** exponent + p2 ** exponent
    return (p1 ** exponent) / z, (p2 ** exponent) / z


def normal_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def cover_prob(margin_mean: float, margin_std: float, home_line: float) -> float:
    """P(home side covers) when the margin (home - away) ~ N(mean, std^2).

    home_line = -6.5 (home favored) -> home covers if margin > 6.5.
    home_line = +3.5 (home dog)    -> home covers if margin > -3.5.
    A push (margin exactly on a whole-number threshold) counts as a loss,
    which is the standard sportsbook settlement rule.
    """
    threshold = -home_line
    return 1.0 - normal_cdf((threshold - margin_mean) / margin_std)


def over_prob(total_mean: float, total_std: float, line: float) -> float:
    """P(game goes over) when the combined score ~ N(mean, std^2)."""
    return 1.0 - normal_cdf((line - total_mean) / total_std)


def expected_value(model_prob: float, decimal_odds: float) -> float:
    """EV per 1.0 unit staked: p*(decimal-1) - (1-p)."""
    b = decimal_odds - 1.0
    return model_prob * b - (1.0 - model_prob)


def kelly_fraction(model_prob: float, decimal_odds: float) -> float:
    """Full Kelly fraction of bankroll to stake (0 if the bet has no edge)."""
    b = decimal_odds - 1.0
    if b <= 0.0:
        return 0.0
    q = 1.0 - model_prob
    f = (model_prob * b - q) / b
    return max(0.0, f)
