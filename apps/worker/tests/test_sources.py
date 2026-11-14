from worker.workflows.sources import _normalize_book_name, _parse_injuries, _parse_news


def test_normalize_book_name_collapses_aliases():
    assert _normalize_book_name("DraftKings - Live Odds") == "DraftKings"
    assert _normalize_book_name("DraftKings") == "DraftKings"
    assert _normalize_book_name("ESPN BET") == "ESPN BET"
    assert _normalize_book_name("  FanDuel - Sportsbook ") == "FanDuel"


def test_parse_injuries():
    data = {
        "athletes": [
            {
                "displayName": "John Doe",
                "position": {"abbreviation": "QB"},
                "injuries": [{"status": "Out", "detail": "Knee surgery"}],
            },
            {
                "displayName": "Jane Roe",
                "position": {"abbreviation": "WR"},
                "injuries": [{"status": "Questionable"}],
            },
            {"displayName": "Healthy Guy", "injuries": []},
        ]
    }
    out = _parse_injuries(data)
    assert out == ["John Doe (QB) - Out: Knee surgery", "Jane Roe (WR) - Questionable"]
    assert _parse_injuries({}) == []
    assert _parse_injuries(None) == []


def test_parse_news():
    data = {"articles": [{"headline": "Coach fired"}, {"headline": "QB named starter"}, {"other": 1}]}
    assert _parse_news(data) == ["Coach fired", "QB named starter"]
    assert _parse_news(data, limit=1) == ["Coach fired"]
    assert _parse_news({}) == []
