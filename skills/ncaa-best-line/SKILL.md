---
name: |
  ncaa-best-line
description: |
  Run the deployed NCAA D1 (FBS) best-line workflow `ncaa-d1-best-line` on the `crb-workflow-worker` Mistral Studio deployment and report the best spread/moneyline/over-under lines with edge, EV, breakeven odds, and Kelly-sized stakes. Projections and the report weigh qualitative team context (injuries, news headlines) pulled live from ESPN. Load this skill whenever the user invokes /ncaa-best-line or asks Vibe to find the best NCAA D1 football betting lines, line-shop college football odds, or run the crb-workflow best-line workflow.
---

# NCAA D1 Best-Line Runner

When the user runs `/ncaa-best-line` (optionally with arguments), execute the managed
workflow deployment in the Mistral Studio workspace and present its output.

## What to run

Use the connected Mistral connector (`mistral_mcp`) tools:

1. Start the execution:
   - `execute_workflow` with
     - `workflow_identifier`: `ncaa-d1-best-line`
     - `deployment_name`: `crb-workflow-worker`
     - `input`: see "Input" below
     - `_confirmationReason`: short reason, e.g. "User ran /ncaa-best-line to get today's best FBS lines."
   - (SDK equivalent: `client.workflows.execute_workflow(workflow_identifier="ncaa-d1-best-line", input={...})`)
2. Poll `get_workflow_execution` with the returned `execution_id` until `status` is
   `COMPLETED` or `FAILED` (use `read_workflow_execution_events` with `wait_seconds`
   between polls; on `FAILED`, report the failure and stop).
3. Present the result to the user (see "Output" below).

## Input

Defaults (omit `games` to pull the **live ESPN FBS slate** automatically):

``cjson
{
  "bankroll": 2500,
  "kelly_fraction": 0.25,
  "max_stake_pct": 0.03,
  "max_exposure_pct": 0.2,
  "max_game_exposure_pct": 0.05,
  "min_edge": 0.01,
  "model": "mistral-large-latest",
  "sources": [{ "type": "espn", "date": "today", "limit": 50, "include_team_context": true }]
}
```

The live ESPN source also pulls per-team injuries and news headlines by default
(`sources[0].include_team_context: true`); the workflow weighs them in its projections
and surfaces per-game team notes in the report.

Optional slash-command arguments the user may pass — map them onto `input`:
- a date like `2026-11-21` or `tomorrow  → `sources[0].date` (`today` default; `tomorrow` = next calendar day, YYYY-MM-DD)
- a number like `bankroll 5000` or `$5000` → `bankroll`
- `limit N` → `sources[0].limit` (cap on games scanned, default 50)
- `game cap 0.06` → `max_game_exposure_pct` (per-game total-stake cap, correlation guard)
- `no team context` → `sources[0].include_team_context = false`
- anything else ambiguous → ask one short clarifying question before running.

## Output

Summarize for the user:
- ingestion info (`result.ingestion`: sources, games with odds, books seen, warnings)
- per game: best line per market (book, line, American odds), consensus fair prob,
  model prob, edge, EV, `breakeven_decimal` (fair price = 1/model_prob), and recommended
  `stake`; highlight picks with `stake > 0`
- per-game team notes from the report (injuries / news that shaped the projection)
- `total_stake` vs `bankroll`, and any `result.warnings` (e.g. neutral fallback projections
  after an LLM failure, or failed injury/news fetches — call these out as degraded inputs)
- the full `report_markdown` (or its key sections if very long)

Always end with the responsible-gambling note: analysis only, no guaranteed outcomes,
only wager what you can afford to lose, 21+ and legal in licensed jurisdictions only,
never chase losses, and 1-800-GAMBLER (US) if gambling is a problem.

## Troubleshooting

- `FAILED` with "No games with odds found" → the slate is empty for that date; tell the
  user and suggest another date.
- `FAILED` with `ReadTimeout` on `build_report` → the worker is running an old revision
  (pre-`a40ab81`, which raised the LLM call timeout to 9 min); check
  `get_workflow_deployment`'s `deployed_revision` for `crb-workflow-worker` and redeploy
  from current `main` if stale.
- Deployment not ready → check `get_workflow_deployment` for `crb-workflow-worker` and
  report its `rollout_status` / recent runtime logs via `get_workflow_deployment_logs`.
