---
name: |
  cn-astroturf-mapper
description: |
  Run the deployed `cn-astroturf-mapper` Mistral Studio workflow on the `cn-astroturf-mapper-worker` deployment to map potential X Community Notes astroturfing / coordinated-rating campaigns against given X handles (targets). Load this skill whenever the user asks to detect, map, or analyze astroturfing campaigns, brigading, or coordinated note ratings targeting specific X accounts, or says to run the cn-astroturf-mapper workflow.
---

# CN Astroturf Mapper Runner

When the user wants to map potential Community Notes astroturfing campaigns
against given X handles, execute the managed workflow deployment in the
Mistral Studio workspace and present its output.

**Privacy guardrail:** the workflow analyzes X's *public* Community Notes
dataset and reports coordination clusters of **pseudonymous participant IDs
only**. Never map those IDs to X handles/accounts, and say so when presenting
results. Findings are hypotheses for human review, not proof of a campaign.

## What to run

Use the connected Mistral connector (`mistral_mcp`) tools:

1. Start the execution:
   - `execute_workflow` with
     - `workflow_identifier`: `cn-astroturf-mapper`
     - `deployment_name`: `cn-astroturf-mapper-worker`
     - `input`: see "Input" below
     - `_confirmationReason`: short reason, e.g. "User asked to map potential Community Notes astroturfing targeting the given handles."
   - (SDK equivalent: `client.workflows.execute_workflow(workflow_identifier="cn-astroturf-mapper", deployment_name="cn-astroturf-mapper-worker", input={...})`)
2. Poll `get_workflow_execution` with the returned `execution_id` until
   `status` is `COMPLETED` or `FAILED` (use `read_workflow_execution_events`
   with `wait_seconds` between polls for progress; on `FAILED`, read
   `get_workflow_execution_logs` and report the failure, then stop).

## Input

```json
{
  "target_handles": ["handle1", "handle2"],
  "lookback_days": 90,
  "max_tweets_per_handle": 500,
  "with_replies": false,
  "with_reposts": false,
  "handle_source": "xmd",
  "min_notes_for_rater": 3,
  "min_co_rating_overlap": 0.5,
  "min_cluster_size": 5,
  "burst_window_minutes": 30,
  "burst_min_ratings": 8,
  "burst_one_sided_ratio": 0.8,
  "detect_flips": true,
  "model": "mistral-large-latest"
}
```

`target_handles` is the only required field. Map user phrasing onto `input`:

- `@somehandle`, `x.com/somehandle`, or bare `somehandle` → append to
  `target_handles` (strip `@` and URLs; if the user names no handle, ask which
  handles are the suspected targets before running).
- "last N days" / "past N weeks" / "N months" → `lookback_days`.
- "include replies" → `with_replies: true`. Never set `with_reposts: true`
  (reposts are other authors' posts).
- "use Grok/xAI x search instead of x.md" → `handle_source: "grok_x_search"`;
  "fall back to it when x.md fails" → `handle_source: "auto"`.
- "more/less sensitive" → lower/raise `min_cluster_size` and
  `min_co_rating_overlap`; "faster/slower burst" → `burst_window_minutes` /
  `burst_min_ratings`.

## Output

Summarize for the user from `result`:

- `result.targets`: handles resolved, `source` used (x.md / grok x_search / auto
  fallback), how many of their posts were scanned, optional `x_posts_fetched`
  (xAI usage when grok x_search ran), and `warning` fields (unreadable/protected
  profiles, missing XAI_API_KEY).
- `result.findings.per_handle[handle]`: `severity` (`none/low/medium/high/
  critical`), `coordination_score` (0–100), `notes_on_their_posts`,
  `clusters` (id, size, mean overlap, pseudonymous member IDs — never resolve
  them), `bursts`, `status_flips`.
- `result.llm_assessment`: per-handle narrative, evidence, recommended
  actions, and methodology caveats.
- `result.report_markdown`: the full markdown report (or its key sections if
  very long).
- Always end with `result.disclaimer` (pseudonymous IDs only; coordination vs.
  organic partisan brigading; human review needed).

## Troubleshooting

- Deployment not ready → `get_workflow_deployment` for
  `cn-astroturf-mapper-worker` and report `ready` / `rollout_status` /
  `active_worker_count`.
- Workflow not registered → `list_workflow_registrations` with
  `workflow_name="cn-astroturf-mapper"`; if absent, the worker code in
  `workflows/cn-astroturf-mapper/worker` of the dt-personal-workflows monorepo
  must be deployed first (managed deployment `cn-astroturf-mapper-worker`,
  dockerfile `deploy/docker/Dockerfile.cn-astroturf-mapper`).
- `FAILED` with an x.md `429` rate-limit message in the logs → the worker needs
  the `X_MD_API_KEY` secret (x.md key, not an X token) or a smaller
  `max_tweets_per_handle` / fewer handles per run. x.md `403`/`404`/`5xx` on a
  single profile only downgrade that handle to a `warning` in
  `result.targets` — the run still completes.
- `FAILED` with missing-data errors → the CN public data is served as daily
  snapshots (`YYYY/MM/DD/<table>/<prefix>-NNNNN.zip` under `CN_DATA_BASE`);
  check `CN_DATA_BASE` / `CN_LOOKBACK_DAYS` / `CN_MAX_SHARDS` if X changed the
  layout.
- `warning` containing "XAI_API_KEY not set" (with `handle_source`
  "grok_x_search"/"auto") → add the `XAI_API_KEY` secret to the
  `cn-astroturf-mapper-worker` deployment — an xAI API key, never an X token or
  grok.com login.
- `FAILED` with an xAI `429` rate-limit message → lower `max_tweets_per_handle`
  or check the xAI team rate limits. x_search is billed per post fetched
  ($5/1k posts), reported per handle as `x_posts_fetched`.
