# NCAA D1 Best-Line Finder (Mistral Studio Workflow)

A [Mistral Workflows](https://docs.mistral.ai/studio/workflows/quickstarts/your-first-workflow) project that answers, for every NCAA Division I (FBS) game on the slate:

> **Where is the best betting line right now — across the spread, the moneyline, and the over/under — and is it worth betting given my own read of the game?**

By default it **pulls the live FBS slate and per-sportsbook odds from ESPN's public JSON APIs** (no API key needed), then line-shops the books, strips the vig for a true consensus price, compares that against a model projection, sizes any stake with fractional Kelly, and finishes with a Markdown report. You can instead pass an explicit slate of games and odds. Every step is a durable workflow **activity**, so an execution resumes from the last completed step if the worker restarts.

> **Disclaimer.** For entertainment and analysis only. Sports betting involves risk and no outcome is guaranteed; model edges are estimates, not certainties. Only wager what you can afford to lose, never chase losses, and use your sportsbook's deposit/loss/time limits. Betting is restricted to adults 21+ and is legal only in licensed jurisdictions — check your local laws before placing any bet. If gambling is a problem, call or text **1-800-GAMBLER** (US).

## What "best line" means here

For each game and each market, the workflow scans every sportsbook and picks, per side:

- **Best price** — the highest decimal odds (e.g. -105 beats -110)
- **Best number** — tie-break on the most favorable line (e.g. -6.5 beats -7 for the favorite; +7 beats +6.5 for the dog; 55.5 beats 56 for the over)

Then it computes:

- **Consensus fair probability** — each book's two-way market is devigged (power method, k=2) and averaged across books
- **Edge** = model probability − consensus fair probability
- **EV per unit** = p·(decimal−1) − (1−p) at the best price found
- **Stake** = fractional Kelly (default ¼ Kelly), capped per bet ("max_stake_pct") and in total ("max_exposure_pct")

## Pipeline

~~~mermaid
flowchart LR
    A[ingest_live_odds<br/>ESPN slate + per-book odds] --> B[normalize_odds<br/>American to decimal + implied]
    B --> C[compute_consensus<br/>devig power k=2, avg across books]
    C --> D[line_shop<br/>best price + best number per side]
    E[project_games<br/>LLM FBS projections] --> F[evaluate_edges<br/>cover/win/over prob to edge, EV]
    D --> F
    F --> G[size_stakes<br/>fractional Kelly + hard caps]
    G --> H[build_report<br/>Markdown report + disclaimer]
~~~

Step 0 runs only when the input omits "games". All steps are "@workflows.activity()" functions orchestrated by one "@workflows.workflow.define" entrypoint ("ncaa-d1-best-line").

## Live odds ingestion (ESPN, default)

The built-in "espn" source needs no API key and uses ESPN's public endpoints:

- Slate: https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard with params "dates" (YYYYMMDD, America/New_York), "groups" (80 = all FBS), "limit", optional "seasontype"
- Per-sportsbook lines: https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{competition_id}/odds — one entry per book with spread, moneyline and over/under in American odds

Each scheduled game with at least one sportsbook becomes a "GameOdds" ("game_id" = "espn-{event_id}"). Finished games are always skipped; in-progress games are skipped unless "include_in_progress" is true. The output's "ingestion" field records what was fetched (sources, fetched_at_utc, events_scanned, games_with_odds, books_seen, detail).

Default input ("examples/sample_input.json"):

~~~json
{
  "bankroll": 2500,
  "kelly_fraction": 0.25,
  "max_stake_pct": 0.03,
  "max_exposure_pct": 0.2,
  "min_edge": 0.01,
  "model": "mistral-large-latest",
  "sources": [
    {"type": "espn", "date": "today", "groups": [80], "limit": 100, "per_book": true, "include_in_progress": false}
  ]
}
~~~

"SourceSpec" fields:

| Field | Default | Meaning |
| --- | --- | --- |
| "type" | "espn" | source id; see SOURCES in sources.py |
| "date" | today (ET) | "YYYY-MM-DD" or "today" |
| "groups" | [80] | ESPN conference-group ids; 80 = all FBS |
| "limit" | 100 | max games scanned |
| "seasontype" | all | 1 = preseason, 2 = regular season, 3 = postseason |
| "per_book" | true | fetch each sportsbook's own lines (core odds API); false = scoreboard consensus only |
| "include_in_progress" | false | also ingest in-progress games |
| "providers" | all | allow-list of sportsbook names (case-insensitive) |
| "timeout_seconds" | 20 | per-request timeout |
| "max_concurrency" | 8 | parallel requests |

To target specific conferences, pass their ESPN conference group ids in "groups" (80 = all FBS). On off-days ESPN returns an empty slate and the workflow fails fast with a clear error.

## Repo layout

~~~
apps/worker/
  pyproject.toml                        # worker package (mistralai + pydantic + httpx)
  src/worker/entrypoints/worker.py      # discovers worker.workflows and runs the worker
  src/worker/workflows/models.py        # input/output pydantic models (incl. SourceSpec)
  src/worker/workflows/sources.py       # live ingestion: ESPN slate + per-book odds
  src/worker/workflows/odds_math.py     # pure odds math (american/decimal, devig, EV, Kelly)
  src/worker/workflows/line_shopper.py  # the 8 activities + workflow class
examples/
  run_execution.py                      # trigger an execution (optional arg: input file)
  sample_input.json                     # live ESPN FBS slate for today (default)
  sample_input_fixed.json               # fixed 3-game x 5-book example slate
.env.example
~~~

## Quickstart

1. **Prereqs:** a Mistral API key, the Mistral CLI ("mistral") with Bun, "uv", Python ≥ 3.12.
2. Clone this repo, then "cp .env.example .env" and set "MISTRAL_API_KEY".
3. From the repo root, start the worker: "mistral dev" (it auto-discovers "ncaa-d1-best-line").
4. In a second terminal: "python examples/run_execution.py" (live slate) or "python examples/run_execution.py examples/sample_input_fixed.json" (fixed slate) — or trigger it from the Studio console, or via the SDK:

~~~python
import os
from mistralai.client import Mistral

client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
execution = client.workflows.execute_workflow(
    workflow_identifier="ncaa-d1-best-line",
    input={"sources": [{"type": "espn", "date": "today"}], "bankroll": 2500},
    wait_for_result=True,
)
print(execution.model_dump_json(indent=2))
~~~

To run the worker without the CLI: "cd apps/worker && uv sync" then "PYTHONPATH=src uv run python -m worker.entrypoints.worker".

## Input schema ("LineShopperInput")

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| "games" | list of "GameOdds" | optional | explicit slate; omit to ingest live games from "sources" |
| "sources" | list of "SourceSpec" | [espn today] | live sources to ingest (see above) |
| "bankroll" | float | 1000 | bankroll used for sizing |
| "kelly_fraction" | float | 0.25 | fraction of full Kelly to stake (¼ Kelly) |
| "max_stake_pct" | float | 0.05 | hard cap on any single bet, as % of bankroll |
| "max_exposure_pct" | float | 0.25 | hard cap on total simultaneous stake, as % of bankroll |
| "min_edge" | float | 0.01 | minimum edge (model prob − fair prob) to size a bet |
| "model" | string | "mistral-large-latest" | model for projections and the report |

"GameOdds": "game_id", "home_team", "away_team", "kickoff_utc", "books" (list of "BookOdds"), optional "projection" (if omitted, the workflow generates one with the LLM). See "examples/sample_input_fixed.json" for a complete explicit-slate example.

"BookOdds": "book", and any of:

- "spread": {home_line, home_price, away_price} — home_line is the home handicap (-6.5 = home favored by 6.5), prices in American odds
- "moneyline": {home_price, away_price}
- "total": {line, over_price, under_price}

## Output schema ("LineShopperOutput")

- "ingestion": what the live fetch returned (sources, fetched_at_utc, events_scanned, games_with_odds, books_seen, detail); null for explicit slates
- "results": per game — "matchup", "projection", "consensus", and "picks": one "BestLine" per market/side with "book", "line", "american_odds", "decimal_odds", "implied_prob", "consensus_fair_prob", "model_prob", "edge", "ev_per_unit", "qualifies", "stake"
- "total_stake", "bankroll"
- "report_markdown": human-readable report
- "disclaimer": responsible-gambling text

## The math

- American → decimal: odds > 0 → 1 + odds/100; odds < 0 → 1 + 100/|odds|
- Implied probability: 1 / decimal (includes vig)
- Devig (power method, k=2): fair_i = p_i^k / (p_1^k + p_2^k) per book, then average across books
- Cover probability: margin ~ N(μ, σ²), P(home covers) = 1 − Φ((−home_line − μ)/σ); away is the complement
- Over probability: total ~ N(μ_t, σ_t²), P(over) = 1 − Φ((line − μ_t)/σ_t)
- EV per unit staked: p·b − (1−p) where b = decimal − 1
- Kelly: f* = (p·b − q)/b; staked fraction = min(f* · kelly_fraction, max_stake_pct), then scaled down if total exposure exceeds max_exposure_pct · bankroll

## Guardrails (built in)

- Per-bet and total-exposure hard caps; only positive-edge picks above "min_edge" get a stake
- Neutral fallback projection if the LLM call fails, with low confidence flagged
- Live ingestion only takes scheduled (or explicitly in-progress) games that have at least one sportsbook; per-request timeouts, retries and a concurrency cap keep the ingest fast and polite
- The report must end with the responsible-gambling disclaimer and may not imply guaranteed wins

## Extending

- **Another live source:** implement fetch_<name>(spec) -> IngestionResult in "apps/worker/src/worker/workflows/sources.py" and register it in SOURCES. Example: The Odds API (https://the-odds-api.com, sport key "americanfootball_ncaaf", markets "spreads,moneylines,totals") with an "ODDS_API_KEY" in .env
- **Alt lines / props:** extend "BookOdds" with alternate spreads/totals or player props
- **Grading the workflow:** log your fill price vs. the closing line (CLV) — positive CLV is the real test of whether best-line picking works

## Disclaimer

This project is for entertainment and analysis only. It does not provide betting advice, guarantees no outcomes, and is not affiliated with any sportsbook, ESPN, or the NCAA. Only wager what you can afford to lose, in jurisdictions where sports betting is legal, 21+. If gambling is a problem, call or text 1-800-GAMBLER (US).
