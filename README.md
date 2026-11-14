# NCAA D1 Best-Line Finder (Mistral Studio Workflow)

A [Mistral Workflows](https://docs.mistral.ai/studio/workflows/quickstarts/your-first-workflow) project that answers one question for every NCAA Division I (FBS) game you care about:

> **Where is the best betting line right now — across the spread, the moneyline, and the over/under — and is it worth betting given my own read of the game?**

It line-shops multiple sportsbooks, strips the bookmaker's vig to get a true consensus price, compares that against a model projection, sizes any stake with fractional Kelly, and finishes with a Markdown report. Every step is a durable workflow **activity**, so an execution resumes from the last completed step if the worker restarts.

> **Disclaimer.** For entertainment and analysis only. Sports betting involves risk and no outcome is guaranteed; model edges are estimates, not certainties. Only wager what you can afford to lose, never chase losses, and use your sportsbook's deposit/loss/time limits. Betting is restricted to adults 21+ and is legal only in licensed jurisdictions — check your local laws before placing any bet. If gambling is a problem, call or text **1-800-GAMBLER** (US).

## What "best line" means here

For each game and each market, the workflow scans every sportsbook you provide and picks, per side:

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
    A[normalize_odds<br/>American → decimal + implied] --> B[compute_consensus<br/>devig power k=2, avg across books]
    B --> C[line_shop<br/>best price + best number per side]
    D[project_games<br/>LLM FBS projections] --> E[evaluate_edges<br/>cover/win/over prob → edge, EV]
    C --> E
    E --> F[size_stakes<br/>fractional Kelly + hard caps]
    F --> G[build_report<br/>Markdown report + disclaimer]
~~~

All seven steps are "@workflows.activity()" functions orchestrated by one "@workflows.workflow.define" entrypoint ("ncaa-d1-best-line").

## Repo layout

~~~
apps/worker/
  pyproject.toml                     # worker package (mistralai + pydantic)
  src/worker/entrypoints/worker.py   # discovers worker.workflows and runs the worker
  src/worker/workflows/models.py     # input/output pydantic models
  src/worker/workflows/odds_math.py  # pure odds math (american/decimal, devig, EV, Kelly)
  src/worker/workflows/line_shopper.py  # the 7 activities + workflow class
examples/
  run_execution.py                   # trigger an execution with the sample input
  sample_input.json                  # 3 games × 5 sportsbooks of example odds
.env.example
~~~

## Quickstart

1. **Prereqs:** a Mistral API key, the Mistral CLI ("mistral") with Bun, "uv", Python ≥ 3.12.
2. Clone this repo, then "cp .env.example .env" and set "MISTRAL_API_KEY".
3. From the repo root, start the worker: "mistral dev" (it auto-discovers "ncaa-d1-best-line").
4. In a second terminal, run an execution: "python examples/run_execution.py" — or trigger it from the Studio console, or via the SDK:

~~~python
import os
from mistralai.client import Mistral

client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
execution = client.workflows.execute_workflow(
    workflow_identifier="ncaa-d1-best-line",
    input={"games": [...], "bankroll": 2500},
    wait_for_result=True,
)
print(execution.model_dump_json(indent=2))
~~~

To run the worker without the CLI: "cd apps/worker && uv sync" then "PYTHONPATH=src uv run python -m worker.entrypoints.worker".

## Input schema ("LineShopperInput")

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| "games" | list of "GameOdds" | required | Games with per-book odds (see below) |
| "bankroll" | float | 1000 | Bankroll used for sizing |
| "kelly_fraction" | float | 0.25 | Fraction of full Kelly to stake (¼ Kelly) |
| "max_stake_pct" | float | 0.05 | Hard cap on any single bet, as % of bankroll |
| "max_exposure_pct" | float | 0.25 | Hard cap on total simultaneous stake, as % of bankroll |
| "min_edge" | float | 0.01 | Minimum edge (model prob − fair prob) to size a bet |
| "model" | string | "mistral-large-latest" | Model for projections and the report |

"GameOdds": "game_id", "home_team", "away_team", "kickoff_utc", "books" (list of "BookOdds"), optional "projection" (if omitted, the workflow generates one with the LLM).

"BookOdds": "book", and any of:

- "spread": {home_line, home_price, away_price} — home_line is the home handicap (-6.5 = home favored by 6.5), prices in American odds
- "moneyline": {home_price, away_price}
- "total": {line, over_price, under_price}

See "examples/sample_input.json" for a complete example.

## Output schema ("LineShopperOutput")

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
- The report must end with the responsible-gambling disclaimer and may not imply guaranteed wins

## Extending

- **Live odds:** add an activity that pulls [The Odds API](https://the-odds-api.com) (sport key "americanfootball_ncaaf", regions "us", markets "spreads,moneylines,totals") and maps the response to "GameOdds"
- **Alt lines / props:** extend "BookOdds" with alternate spreads/totals or player props
- **Grading the workflow:** log your fill price vs. the closing line (CLV) — positive CLV is the real test of whether best-line picking works

## Disclaimer

This project is for entertainment and analysis only. It does not provide betting advice, guarantees no outcomes, and is not affiliated with any sportsbook or the NCAA. Only wager what you can afford to lose, in jurisdictions where sports betting is legal, 21+. If gambling is a problem, call or text 1-800-GAMBLER (US).
