"""Pydantic models for the NCAA D1 best-line workflow (input, intermediates, output)."""
from __future__ import annotations

from pydantic import BaseModel, Field


# ------------------------------- input -------------------------------


class SpreadOdds(BaseModel):
    """Point-spread market at one sportsbook.

    home_line is the home team's handicap: -6.5 means the home team is a 6.5-point
    favorite; +3.5 means the home team is a 3.5-point underdog. Prices are American odds.
    """

    home_line: float
    home_price: int
    away_price: int


class MoneylineOdds(BaseModel):
    """Moneyline (win outright) prices, American odds."""

    home_price: int
    away_price: int


class TotalOdds(BaseModel):
    """Over/under market at one sportsbook."""

    line: float
    over_price: int
    under_price: int


class BookOdds(BaseModel):
    """One sportsbook's snapshot for a game. Any market may be omitted."""

    book: str
    spread: SpreadOdds | None = None
    moneyline: MoneylineOdds | None = None
    total: TotalOdds | None = None
    last_update_utc: str = ""


class Projection(BaseModel):
    """Model projection for one game, from the home team's perspective.

    Optional input per game; if omitted, project_games generates one with an LLM.
    """

    expected_margin: float = Field(..., description="Expected home-minus-away margin (points)")
    margin_std: float = Field(..., gt=0, description="Std dev of margin (FBS games: ~13-17)")
    expected_total: float = Field(..., gt=0, description="Expected combined score")
    total_std: float = Field(..., gt=0, description="Std dev of total (FBS games: ~10-13)")
    home_win_prob: float = Field(..., ge=0.0, le=1.0)
    confidence: float = Field(0.5, ge=0.0, le=1.0)
    notes: str = ""


class GameOdds(BaseModel):
    game_id: str
    home_team: str
    away_team: str
    kickoff_utc: str = ""
    books: list[BookOdds] = Field(..., min_length=1)
    projection: Projection | None = None


class SourceSpec(BaseModel):
    """One live odds source. The built-in type is "espn" (public ESPN JSON APIs, no key)."""

    type: str = "espn"
    date: str | None = None  # "YYYY-MM-DD" or "today"; None = today in America/New_York
    groups: list[int] | None = None  # ESPN conference-group ids; None or [80] = all FBS
    limit: int = Field(100, ge=1, le=500)  # max games scanned from the slate
    seasontype: int | None = None  # 1 = preseason, 2 = regular season, 3 = postseason
    per_book: bool = True  # fetch each sportsbook's own lines via the ESPN core odds API
    include_in_progress: bool = False  # also ingest games already in progress
    providers: list[str] | None = None  # keep only these sportsbooks (case-insensitive)
    timeout_seconds: float = Field(20.0, gt=0)
    max_concurrency: int = Field(8, ge=1, le=32)


class IngestionInfo(BaseModel):
    """Provenance for a live ingestion step."""

    sources: list[str] = Field(default_factory=list)
    fetched_at_utc: str = ""
    events_scanned: int = 0
    games_with_odds: int = 0
    books_seen: list[str] = Field(default_factory=list)
    detail: str = ""


class IngestionResult(BaseModel):
    games: list[GameOdds] = Field(default_factory=list)
    info: IngestionInfo = Field(default_factory=IngestionInfo)


class LineShopperInput(BaseModel):
    games: list[GameOdds] | None = Field(
        None, description="Explicit slate with per-book odds; omit to pull live games from sources"
    )
    sources: list[SourceSpec] | None = Field(
        None, description="Live sources to ingest; default = ESPN FBS slate for today"
    )
    bankroll: float = Field(1000.0, gt=0)
    kelly_fraction: float = Field(0.25, gt=0, le=1, description="Fraction of full Kelly to stake (0.25 = quarter Kelly)")
    max_stake_pct: float = Field(0.05, gt=0, le=0.25, description="Hard cap on any single bet, as a fraction of bankroll")
    max_exposure_pct: float = Field(0.25, gt=0, le=1, description="Hard cap on total simultaneous stake, as a fraction of bankroll")
    min_edge: float = Field(0.01, ge=0, description="Minimum edge (model prob - fair prob) required to size a bet")
    model: str = "mistral-large-latest"


# ------------------------------- output -------------------------------


class BestLine(BaseModel):
    """The best available line for one market/side, plus edge/EV and stake."""

    market: str  # "spread" | "moneyline" | "total"
    side: str  # "home" | "away" | "over" | "under"
    label: str  # human-readable, e.g. "Ohio State Buckeyes -6.5"
    book: str
    line: float | None  # spread/total number attached to the pick (None for moneyline)
    american_odds: int
    decimal_odds: float
    implied_prob: float  # raw implied prob at the best price (includes vig)
    consensus_fair_prob: float  # devigged market consensus for this side
    model_prob: float  # model probability of this side winning the bet
    edge: float  # model_prob - consensus_fair_prob
    ev_per_unit: float  # expected value per 1.0 unit staked at the best price
    qualifies: bool  # edge >= min_edge
    stake: float  # recommended stake in currency units (0 if not qualifying)


class GameResult(BaseModel):
    game_id: str
    matchup: str
    projection: Projection | None = None
    picks: list[BestLine]
    consensus: dict


class LineShopperOutput(BaseModel):
    workflow: str = "ncaa-d1-best-line"
    bankroll: float
    total_stake: float
    results: list[GameResult]
    report_markdown: str
    disclaimer: str
    ingestion: IngestionInfo | None = None


# --------------------------- LLM structured output ---------------------------


class GameProjection(BaseModel):
    game_id: str
    projection: Projection


class GameProjections(BaseModel):
    projections: list[GameProjection]
