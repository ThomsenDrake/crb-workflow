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
   `get_workflow_execution_logs` and report the failure, then stop). On each
   poll, also run `query_workflow_execution` with `query_name`
   `__get_pending_inputs`: with `handle_source` "grok_cli"/"auto" the run is
   EXPECTED to pause once on a device-login pending input — drive it as below.

## Grok device-code login (expected with handle_source "grok_cli"/"auto")

The worker image ships the Grok Build CLI (`GROK_BIN=grok`). The first
`grok_cli`/`auto` run per container has no cached login, so the execution
pauses on a pending input. Drive the RFC 8628 device-code flow from the client:

1. `query_workflow_execution` with `query_name` `__get_pending_inputs` —
   find the entry whose `label` starts with
   "grok device login: open <url> and enter code <code>" (same info via
   `get_grok_auth`).
2. Tell the user to open <url> in a browser and enter <code> with their
   grok.com / X login. The code is a confirmation echo, never a credential.
3. Submit: `update_workflow_execution` with `update_name` `__submit_input`,
   `input` `{"task_id": <task_id>, "input": {"user_code": "<code>"}}`.
4. The background `grok login --device-auth` poll then completes and writes
   `GROK_HOME/auth.json`; `get_grok_auth` shows `status: "authenticated"` and
   the run resumes. If the 10-minute input window lapses, the run finishes
   with a per-handle `warning` instead of failing.

## Input

```json
{
  "target_handles": ["handle1", "handle2"],
  "lookback_days": 90,
  "max_tweets_per_handle": 500,
  "with_replies": false,
  "with_reposts": false,
  "handle_source": "auto",  // default; "xmd" (x.md only) | "grok_x_search" (xAI API) | "grok_cli" (Grok Build CLI headless)
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
- Default is `handle_source: "auto"` — the run pauses once per container for the
  grok device-code login (see above), then resolves via x.md and falls back to
  Grok per handle when x.md downgrades it. "use x.md only / skip grok" →
  `handle_source: "xmd"`.
- "use Grok/xAI x search instead of x.md" → `handle_source: "grok_cli"` (Grok
  Build CLI shipped in the worker image; EXPECT the device-code login pause,
  see above) or `"grok_x_search"` (xAI Responses API; needs the `XAI_API_KEY`
  secret on the deployment).
- "fall back to Grok when x.md fails" → `handle_source: "auto"` (x.md first,
  then grok_x_search if keyed, then grok_cli).
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
- `warning` containing "grok CLI not found" / "grok CLI exited" (with
  `handle_source` "grok_cli"/"auto") → the worker image must include the grok
  binary (see `deploy/docker/Dockerfile.cn-astroturf-mapper`, `GROK_BIN=grok`);
  if the binary is present but login failed, re-run and complete the device-code
  pending input when it appears (see above). The workflow never reads or logs
  the token file.
