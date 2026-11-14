"""NCAA D1 best-line workflow: spread / moneyline / over-under line shopping.

Pipeline — every step is a durable activity, so an execution resumes from the last
completed step if the worker restarts:

1. normalize_odds     American -> decimal odds + implied probabilities, per book/market
2. compute_consensus  devig every two-way market (power method) and average across books
3. line_shop          best price (and best number) per market/side across all books
4. project_games      LLM projections (margin/total mean+std, win prob) or caller-supplied
5. evaluate_edges     model cover/win/over probabilities vs consensus fair probs -> edge, EV
6. size_stakes        fractional Kelly with per-bet and total-exposure hard caps
7. build_report       LLM-written Markdown report with responsible-gambling disclaimer
"""
from __future__ import annotations

import json
import statistics
from datetime import timedelta

import mistralai.workflows as workflows
from mistralai.workflows import Depends
from mistralai.workflows.client import get_mistral_client

from . import odds_math as om
from .models import (
    BestLine,
    GameProjection,
    GameProjections,
    GameResult,
    LineShopperInput,
    LineShopperOutput,
    Projection,
)

DISCLAIMER = (
    "For entertainment and analysis only. Sports betting involves risk and no outcome is "
    "guaranteed; model edges are estimates, not predictions of certainty. Only wager what you "
    "can afford to lose, never chase losses, and use your sportsbook's deposit/loss/time limits. "
    "Betting is restricted to adults 21+ and is legal only in licensed jurisdictions - check your "
    "local laws before placing any bet. If gambling is a problem, call or text 1-800-GAMBLER (US)."
)


def _side(american: int) -> dict:
    decimal = om.american_to_decimal(american)
    return {
        "american": american,
        "decimal": round(decimal, 6),
        "implied": round(om.implied_prob(decimal), 6),
    }


@workflows.activity(start_to_close_timeout=timedelta(minutes=2))
async def normalize_odds(input: LineShopperInput) -> list[dict]:
    """Convert every sportsbook's American odds to decimal odds and implied probabilities."""
    games = []
    for g in input.games:
        books = []
        for b in g.books:
            entry: dict = {"book": b.book, "last_update_utc": b.last_update_utc}
            if b.spread is not None:
                entry["spread"] = {
                    "home_line": b.spread.home_line,
                    "home": _side(b.spread.home_price),
                    "away": _side(b.spread.away_price),
                }
            if b.moneyline is not None:
                entry["moneyline"] = {
                    "home": _side(b.moneyline.home_price),
                    "away": _side(b.moneyline.away_price),
                }
            if b.total is not None:
                entry["total"] = {
                    "line": b.total.line,
                    "over": _side(b.total.over_price),
                    "under": _side(b.total.under_price),
                }
            books.append(entry)
        games.append(
            {
                "game_id": g.game_id,
                "home_team": g.home_team,
                "away_team": g.away_team,
                "kickoff_utc": g.kickoff_utc,
                "matchup": f"{g.away_team} @ {g.home_team}",
                "books": books,
            }
        )
    return games


@workflows.activity(start_to_close_timeout=timedelta(minutes=2))
async def compute_consensus(normalized: list[dict]) -> list[dict]:
    """Devig each two-way market per book (power method) and average fair probs across books."""
    out = []
    for g in normalized:
        spread = None
        spread_books = [b for b in g["books"] if "spread" in b]
        if spread_books:
            fh, fa, lines = [], [], []
            for b in spread_books:
                a, c = om.devig_two_way(b["spread"]["home"]["implied"], b["spread"]["away"]["implied"])
                fh.append(a)
                fa.append(c)
                lines.append(b["spread"]["home_line"])
            spread = {
                "line": statistics.median(lines),
                "home_fair": sum(fh) / len(fh),
                "away_fair": sum(fa) / len(fa),
                "n_books": len(lines),
            }
        ml = None
        ml_books = [b for b in g["books"] if "moneyline" in b]
        if ml_books:
            fh, fa = [], []
            for b in ml_books:
                a, c = om.devig_two_way(b["moneyline"]["home"]["implied"], b["moneyline"]["away"]["implied"])
                fh.append(a)
                fa.append(c)
            ml = {"home_fair": sum(fh) / len(fh), "away_fair": sum(fa) / len(fa), "n_books": len(fh)}
        total = None
        total_books = [b for b in g["books"] if "total" in b]
        if total_books:
            fo, fu, lines = [], [], []
            for b in total_books:
                a, c = om.devig_two_way(b["total"]["over"]["implied"], b["total"]["under"]["implied"])
                fo.append(a)
                fu.append(c)
                lines.append(b["total"]["line"])
            total = {
                "line": statistics.median(lines),
                "over_fair": sum(fo) / len(fo),
                "under_fair": sum(fu) / len(fu),
                "n_books": len(lines),
            }
        out.append({"game_id": g["game_id"], "spread": spread, "moneyline": ml, "total": total})
    return out


@workflows.activity(start_to_close_timeout=timedelta(minutes=2))
async def line_shop(normalized: list[dict], consensus: list[dict]) -> list[dict]:
    """Pick the best available line per market/side: max decimal odds (best price),
    tie-broken by the most favorable number (line)."""
    cons_by_game = {c["game_id"]: c for c in consensus}
    out = []
    for g in normalized:
        cons = cons_by_game[g["game_id"]]
        books = g["books"]
        picks: list[dict] = []

        if cons["spread"]:
            # home: higher home_line is better for the home bettor; away: higher away_line (= -home_line)
            home_c = [(b["spread"]["home"]["decimal"], b["spread"]["home_line"], b) for b in books if "spread" in b]
            away_c = [(b["spread"]["away"]["decimal"], -b["spread"]["home_line"], b) for b in books if "spread" in b]
            bh = max(home_c, key=lambda t: (t[0], t[1]))
            ba = max(away_c, key=lambda t: (t[0], t[1]))
            sb, sa = bh[2]["spread"], ba[2]["spread"]
            picks.append({
                "market": "spread", "side": "home",
                "label": f"{g['home_team']} {sb['home_line']:+g}",
                "book": bh[2]["book"], "line": sb["home_line"],
                "american": sb["home"]["american"], "decimal": sb["home"]["decimal"],
                "implied": sb["home"]["implied"],
                "consensus_fair_prob": cons["spread"]["home_fair"],
            })
            picks.append({
                "market": "spread", "side": "away",
                "label": f"{g['away_team']} {-sa['home_line']:+g}",
                "book": ba[2]["book"], "line": -sa["home_line"],
                "american": sa["away"]["american"], "decimal": sa["away"]["decimal"],
                "implied": sa["away"]["implied"],
                "consensus_fair_prob": cons["spread"]["away_fair"],
            })

        if cons["moneyline"]:
            bh = max(((b["moneyline"]["home"], b) for b in books if "moneyline" in b), key=lambda t: t[0]["decimal"])
            ba = max(((b["moneyline"]["away"], b) for b in books if "moneyline" in b), key=lambda t: t[0]["decimal"])
            picks.append({
                "market": "moneyline", "side": "home",
                "label": f"{g['home_team']} ML",
                "book": bh[1]["book"], "line": None,
                "american": bh[0]["american"], "decimal": bh[0]["decimal"], "implied": bh[0]["implied"],
                "consensus_fair_prob": cons["moneyline"]["home_fair"],
            })
            picks.append({
                "market": "moneyline", "side": "away",
                "label": f"{g['away_team']} ML",
                "book": ba[1]["book"], "line": None,
                "american": ba[0]["american"], "decimal": ba[0]["decimal"], "implied": ba[0]["implied"],
                "consensus_fair_prob": cons["moneyline"]["away_fair"],
            })

        if cons["total"]:
            # over: lower line is better; under: higher line is better
            bo = max(((b["total"]["over"]["decimal"], -b["total"]["line"], b) for b in books if "total" in b), key=lambda t: (t[0], t[1]))
            bu = max(((b["total"]["under"]["decimal"], b["total"]["line"], b) for b in books if "total" in b), key=lambda t: (t[0], t[1]))
            to, tu = bo[2]["total"], bu[2]["total"]
            picks.append({
                "market": "total", "side": "over",
                "label": f"Over {to['line']:g}",
                "book": bo[2]["book"], "line": to["line"],
                "american": to["over"]["american"], "decimal": to["over"]["decimal"], "implied": to["over"]["implied"],
                "consensus_fair_prob": cons["total"]["over_fair"],
            })
            picks.append({
                "market": "total", "side": "under",
                "label": f"Under {tu['line']:g}",
                "book": bu[2]["book"], "line": tu["line"],
                "american": tu["under"]["american"], "decimal": tu["under"]["decimal"], "implied": tu["under"]["implied"],
                "consensus_fair_prob": cons["total"]["under_fair"],
            })

        out.append({"game_id": g["game_id"], "matchup": g["matchup"], "picks": picks})
    return out


@workflows.activity(start_to_close_timeout=timedelta(minutes=10))
async def project_games(
    input: LineShopperInput,
    model: str,
    client=Depends(get_mistral_client),
) -> list[GameProjection]:
    """Build a projection per game: caller-supplied if present, otherwise LLM-generated."""
    result: dict[str, Projection] = {
        g.game_id: g.projection for g in input.games if g.projection is not None
    }
    missing = [
        {
            "game_id": g.game_id,
            "home_team": g.home_team,
            "away_team": g.away_team,
            "kickoff_utc": g.kickoff_utc,
        }
        for g in input.games
        if g.projection is None
    ]
    if missing:
        system = (
            "You are an NCAA Division I (FBS) college football analyst. For each game, estimate "
            "team strength and produce a projection from the HOME team's perspective: "
            "expected_margin (home minus away, points), margin_std (use 13-17 for FBS), "
            "expected_total (combined points), total_std (use 10-13 for FBS), home_win_prob "
            "(consistent with P(margin > 0) under a normal distribution), confidence in [0,1], "
            "and short notes. Do not reference betting lines or odds - you do not have them. "
            "Be honest about uncertainty. Return structured output only."
        )
        user = "Games (JSON):\n" + json.dumps(missing, indent=2)
        try:
            resp = await client.chat.parse_async(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                response_format=GameProjections,
            )
            parsed = resp.choices[0].message.parsed
            for gp in parsed.projections:
                result[gp.game_id] = gp.projection
        except Exception:
            pass  # fall through to neutral defaults below
    for m in missing:
        if m["game_id"] not in result:
            result[m["game_id"]] = Projection(
                expected_margin=0.0,
                margin_std=14.0,
                expected_total=48.0,
                total_std=11.0,
                home_win_prob=0.5,
                confidence=0.2,
                notes="Fallback neutral projection (LLM call failed or omitted); treat edge estimates as unreliable.",
            )
    return [GameProjection(game_id=gid, projection=proj) for gid, proj in result.items()]


@workflows.activity(start_to_close_timeout=timedelta(minutes=2))
async def evaluate_edges(
    best_lines: list[dict], projections: list[GameProjection], min_edge: float
) -> list[dict]:
    """Model probability per pick vs consensus fair probability -> edge and EV."""
    proj_by_game = {p.game_id: p.projection for p in projections}
    out = []
    for g in best_lines:
        proj = proj_by_game.get(g["game_id"])
        picks = []
        for p in g["picks"]:
            if proj is None:
                model_prob = p["consensus_fair_prob"]  # no independent view -> no edge
            elif p["market"] == "spread":
                home_line = p["line"] if p["side"] == "home" else -p["line"]
                home_cover = om.cover_prob(proj.expected_margin, proj.margin_std, home_line)
                model_prob = home_cover if p["side"] == "home" else 1.0 - home_cover
            elif p["market"] == "moneyline":
                model_prob = proj.home_win_prob if p["side"] == "home" else 1.0 - proj.home_win_prob
            else:  # total
                over = om.over_prob(proj.expected_total, proj.total_std, p["line"])
                model_prob = over if p["side"] == "over" else 1.0 - over
            edge = model_prob - p["consensus_fair_prob"]
            picks.append(
                {
                    **p,
                    "model_prob": round(model_prob, 6),
                    "edge": round(edge, 6),
                    "ev_per_unit": round(om.expected_value(model_prob, p["decimal"]), 6),
                    "qualifies": edge >= min_edge,
                }
            )
        out.append({"game_id": g["game_id"], "matchup": g["matchup"], "picks": picks})
    return out


@workflows.activity(start_to_close_timeout=timedelta(minutes=2))
async def size_stakes(
    evaluated: list[dict],
    bankroll: float,
    kelly_fraction: float,
    max_stake_pct: float,
    max_exposure_pct: float,
    projections: list[GameProjection],
    consensus: list[dict],
) -> list[GameResult]:
    """Fractional Kelly stakes with a per-bet hard cap and a total-exposure cap."""
    proj_by_game = {p.game_id: p.projection for p in projections}
    cons_by_game = {c["game_id"]: c for c in consensus}
    results: list[GameResult] = []
    for g in evaluated:
        picks: list[BestLine] = []
        for p in g["picks"]:
            stake = 0.0
            if p["qualifies"] and p["edge"] > 0:
                f = om.kelly_fraction(p["model_prob"], p["decimal"])
                stake = round(min(f * kelly_fraction, max_stake_pct) * bankroll, 2)
            picks.append(BestLine(**{**p, "stake": stake}))
        results.append(
            GameResult(
                game_id=g["game_id"],
                matchup=g["matchup"],
                projection=proj_by_game.get(g["game_id"]),
                picks=picks,
                consensus=cons_by_game.get(g["game_id"], {}),
            )
        )
    total = sum(p.stake for r in results for p in r.picks)
    cap = max_exposure_pct * bankroll
    if total > cap > 0:
        scale = cap / total
        for r in results:
            for p in r.picks:
                p.stake = round(p.stake * scale, 2)
    return results


@workflows.activity(start_to_close_timeout=timedelta(minutes=10))
async def build_report(
    input: LineShopperInput,
    results: list[GameResult],
    model: str,
    client=Depends(get_mistral_client),
) -> str:
    """LLM-written Markdown report; numbers come only from the structured results."""
    system = (
        "You write concise Markdown betting-analysis reports from structured JSON. Rules: use "
        "ONLY the numbers in the provided JSON and never invent figures; for each game show the "
        "best line per market (book, line, American odds), the devigged consensus fair probability, "
        "the model probability, edge, EV per unit staked, and recommended stake; sort picks by edge "
        "descending; include a short methodology section (line shopping across books, power-method "
        "devig k=2, normal-approximation cover/over probabilities, fractional Kelly with caps) and a "
        "'Risks & guardrails' section; end with the provided disclaimer verbatim. Never promise or "
        "imply guaranteed wins."
    )
    payload = {
        "bankroll": input.bankroll,
        "games": [r.model_dump(mode="json") for r in results],
        "disclaimer": DISCLAIMER,
    }
    resp = await client.chat.complete_async(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(payload, indent=2)},
        ],
    )
    return resp.choices[0].message.content


@workflows.workflow.define(
    name="ncaa-d1-best-line",
    workflow_display_name="NCAA D1 Best-Line Finder",
    workflow_description=(
        "Finds the best NCAA Division 1 (FBS) football betting line across point spreads, "
        "moneylines and over/unders: normalizes multi-book odds, removes the vig for a consensus "
        "fair price, line-shops the best price/number per market, estimates edge and EV against a "
        "model projection, sizes stakes with fractional Kelly under hard caps, and returns a "
        "Markdown report with responsible-gambling guardrails."
    ),
)
class NcaaD1BestLineWorkflow:
    @workflows.workflow.entrypoint
    async def run(self, input: LineShopperInput) -> LineShopperOutput:
        normalized = await normalize_odds(input)
        consensus = await compute_consensus(normalized)
        best_lines = await line_shop(normalized, consensus)
        projections = await project_games(input, input.model)
        evaluated = await evaluate_edges(best_lines, projections, input.min_edge)
        results = await size_stakes(
            evaluated,
            input.bankroll,
            input.kelly_fraction,
            input.max_stake_pct,
            input.max_exposure_pct,
            projections,
            consensus,
        )
        report = await build_report(input, results, input.model)
        total_stake = round(sum(p.stake for r in results for p in r.picks), 2)
        return LineShopperOutput(
            bankroll=input.bankroll,
            total_stake=total_stake,
            results=results,
            report_markdown=report,
            disclaimer=DISCLAIMER,
        )
