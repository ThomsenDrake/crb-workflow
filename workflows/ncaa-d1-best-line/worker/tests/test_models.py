from worker.workflows.models import (
    BestLine,
    GameOdds,
    BookOdds,
    SourceSpec,
    TeamContext,
)


def _best_line_kwargs():
    return dict(
        market="spread",
        side="home",
        label="TCU -14",
        book="DraftKings",
        line=-14.0,
        american_odds=-110,
        decimal_odds=1.909091,
        implied_prob=0.52381,
        breakeven_decimal=4.318,
        consensus_fair_prob=0.5,
        model_prob=0.231678,
        edge=-0.268322,
        ev_per_unit=-0.557706,
        qualifies=False,
        stake=0.0,
    )


def test_best_line_requires_breakeven_decimal():
    kwargs = _best_line_kwargs()
    bl = BestLine(**kwargs)
    assert bl.breakeven_decimal == 4.318
    kwargs.pop("breakeven_decimal")
    try:
        BestLine(**kwargs)
        assert False, "expected ValidationError"
    except Exception:
        pass


def test_game_odds_carries_team_context():
    g = GameOdds(
        game_id="espn-1",
        home_team="Home",
        away_team="Away",
        books=[BookOdds(book="DraftKings")],
        home_context=TeamContext(injuries=["John Doe (QB) - Out: Knee"], news=["Coach fired"]),
    )
    assert g.home_context.injuries == ["John Doe (QB) - Out: Knee"]
    assert g.away_context is None


def test_source_spec_team_context_default():
    assert SourceSpec(type="espn").include_team_context is True
