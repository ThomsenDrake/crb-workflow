# CN Astroturf Mapper (Mistral Studio Workflow)

> This workflow is one self-contained package in the personal Mistral workflows monorepo — see the [repo-root README](../../README.md).

A [Mistral Workflows](https://docs.mistral.ai/studio/workflows/quickstarts/your-first-workflow) project that maps **potential** X Community Notes astroturfing / coordinated-rating campaigns against a given list of X handles (the potential *targets*).

It uses only public data: X's [public Community Notes dataset](https://communitynotes.x.com/guide/en/under-the-hood/download-data) plus, for resolving handles to their recent post IDs, the read-only [x.md](https://mdfromx.com/docs) mirror or optionally xAI's Grok X Search tool (`handle_source`, see below).

> **Privacy guardrail (do not remove).** The public dataset exposes raters and note authors only as hashed pseudonymous `participantId`s. This workflow **never** maps participantIds back to X handles or accounts — it reports coordination *clusters of pseudonymous IDs*, never identities. Re-identification would violate X's data terms; attribution requires external data and is out of scope. Findings are hypotheses for human review, not proof of a campaign.

## Pipeline

~~~mermaid
flowchart LR
    A[resolve_target_handles<br/>x.md or xAI x_search: handles -> recent tweet IDs] --> B[load_target_notes<br/>CN notes attached to those tweets]
    B --> C[load_ratings_and_status<br/>CN ratings + status history, streamed]
    C --> D[detect_coordination<br/>co-rating clusters, bursts, status flips, per-handle score]
    D --> E[llm_campaign_assessment<br/>structured narrative per handle]
    E --> F[report_markdown]
~~~

All steps are `@workflows.activity()` functions (durable + retried, so an execution resumes after a worker restart); the `@workflows.workflow.define(name="cn-astroturf-mapper")` entrypoint stays deterministic. The `get_steps` query exposes live per-step progress.

**Methodology (backward audit):** the CN notes table has no tweet-author column (verified against the live shard header: `noteId, noteAuthorParticipantId, createdAtMillis, tweetId, …`), so the noted-tweet set is built by joining handle → post IDs (full history when `history_scope="full"`) against the full notes snapshot: `load_target_notes` keeps every note whose `tweetId` is one of the target's posts — i.e. all of the target's community-noted tweets — and the coordination audit (ratings/status filtered to those note IDs) runs on exactly that set. Per-handle `findings.*.noted_tweet_ids` lists the audited tweets.

## Data sources

- **x.md** (`https://mdfromx.com/docs`, or a self-hosted instance via `X_MD_BASE_URL` — the worker image defaults to `https://xmd.drake-t.workers.dev`) — keyless read-only mirror of public X content. Bulk history route: `GET https://mdfromx.com/api/v1/profiles/{handle}/posts?since=YYYY-MM-DD&max_posts=N&format=json&with_replies=..&with_reposts=false`. Each post's `id` is the numeric tweet ID — exactly what joins to the CN dataset's `tweetId`. `meta.truncated` / `meta.next_until` paginate the walk; 429s fail with an actionable error, while 403/404/5xx responses only downgrade that handle to a per-handle `warning` so one bad profile doesn't sink the whole run. An optional `X_MD_API_KEY` (sent as `Authorization: Bearer`) raises the import allowance (without a key: ~3 imports/hour, 5/day per IP). It is **not** an X API token.
- **xAI Grok X Search (optional handle sources)** — used when `handle_source` is `"grok_x_search"` / `"grok_cli"` (only) or `"auto"` (x.md first, per-handle Grok fallback when x.md downgrades that handle to a warning). Two routes to the same `x_search` capability, both on xAI's side of the xAI–X partnership (X's own ToS "published interfaces" clause governs X's services, e.g. the private GraphQL API — not xAI's):
  - `"grok_x_search"` — the published xAI Responses API: `POST https://api.x.ai/v1/responses` with the `x_search` tool scoped by `allowed_x_handles` + `from_date`/`to_date` (`XAI_API_KEY`, model `XAI_XSEARCH_MODEL`, default `grok-4.7`). Billed per post fetched ($5/1k posts as of 2026-09, surfaced as `x_posts_fetched`) plus tokens — an xAI meter, **not** X's pay-per-use API. xAI 429 fails the run with an actionable message; 401/403/5xx or a missing key only downgrade that handle to a `warning`.
  - `"grok_cli"` — the Grok Build CLI in headless mode (`grok -p …`, documented by xAI for scripts/CI), using the CLI's own cached login (`GROK_HOME`/`~/.grok/auth.json`, e.g. from `grok login --device-code`) or `XAI_API_KEY`. Draws on the Grok subscription pool rather than per-post API billing; the workflow never reads, prints, or passes those credentials (`GROK_BIN` locates the binary). Note xAI's Acceptable Use Policy bans *unauthorized* automated access — keep runs scoped and low-volume; xAI's docs pair CI/headless automation with `XAI_API_KEY`.
  - When `handle_source` is `"grok_cli"`/`"auto"` and the worker has no cached login, the run pauses on a pending input (`__get_pending_inputs` query; label starts with `grok device login: open <url> and enter code <code>`). Open the URL, enter the code, then submit it via the `__submit_input` update (`{"task_id": ..., "input": {"user_code": "<code>"}}`); the background `grok login --device-auth` poll then completes and writes the session to `GROK_HOME`/`~/.grok/auth.json`. `get_grok_auth` exposes the status; the result's `grok_auth` field reports it too. The workflow never reads or returns token material.
- **X public Community Notes dataset** — zipped TSV shards under `CN_DATA_BASE` (default `https://ton.twimg.com/birdwatch-public-data`), organized as **daily snapshots**: `YYYY/MM/DD/notes/notes-NNNNN.zip`, `YYYY/MM/DD/noteRatings/ratings-NNNNN.zip` (~184M rows total — always streamed to a temp file and filtered by note ID, never loaded wholesale), and `YYYY/MM/DD/noteStatusHistory/noteStatusHistory-NNNNN.zip`. The worker walks back up to `CN_LOOKBACK_DAYS` (default 7) days from today to find the latest snapshot; `CN_MAX_SHARDS` caps how many shard indexes are probed per table (default 64). If nothing responds, that table is treated as empty instead of failing the run.

## Detection signals

- **Co-rating clusters** — raters with ≥ `min_notes_for_rater` ratings on target notes are linked (union-find) when the Jaccard overlap of their rated-note sets is ≥ `min_co_rating_overlap`; clusters with ≥ `min_cluster_size` members are flagged and reported as pseudonymous `member_participant_ids`.
- **One-sided rating bursts** — sliding `burst_window_minutes` window containing ≥ `burst_min_ratings` ratings with ≥ `burst_one_sided_ratio` in the same direction (helpful / not-helpful).
- **Status flips** (`detect_flips`) — notes that reached `CURRENTLY_RATED_HELPFUL` and later lost it (post-display downvote raids).
- **Per-handle score** — 0–100 combination of the above → `severity` none/low/medium/high/critical; an LLM (`model`) then writes the per-handle verdict, evidence bullets, recommended actions, and methodology caveats.

## Layout (this workflow's slice of the monorepo)

~~~
workflows/cn-astroturf-mapper/                 # this workflow; paths below are relative to this directory
  worker/
    pyproject.toml                             # worker package (mistralai-workflows[mistralai] + pydantic + httpx)
    src/worker/entrypoints/worker.py           # discovers worker.workflows and runs the worker
    src/worker/workflows/cn_astroturf_mapper.py # the 5 activities + workflow class + pure helpers
    tests/test_detect.py                       # pytest suite for the pure detection helpers
  examples/
    run_execution.py                           # trigger an execution (optional arg: input file)
    sample_input.json                          # example target handles
  skill/SKILL.md                               # Vibe skill (/cn-astroturf-mapper) that runs the deployed workflow
deploy/docker/Dockerfile.cn-astroturf-mapper   # repo-root managed-deployment image (build context = repo root)
.env.example                                   # repo-root env template (X_MD_API_KEY, CN_DATA_BASE, CN_MAX_SHARDS)
~~~

## Quickstart (run the worker locally)

1. **Prereqs:** a Mistral API key, the Mistral CLI ("mistral") with Bun, "uv", Python ≥ 3.12.
2. Clone this repo, then "cp .env.example .env" and set "MISTRAL_API_KEY" (optionally "X_MD_API_KEY").
3. From the repo root, start the worker: "mistral dev" (it auto-discovers "cn-astroturf-mapper").
4. In a second terminal: "python workflows/cn-astroturf-mapper/examples/run_execution.py" (edit "examples/sample_input.json" to set your target handles first).

To run the worker without the CLI: "cd workflows/cn-astroturf-mapper/worker && uv sync" then "PYTHONPATH=src uv run python -m worker.entrypoints.worker".

## Deploy to your Mistral workspace (managed deployment)

Create a managed deployment pointing at this repo — Mistral Cloud builds "deploy/docker/Dockerfile.cn-astroturf-mapper" (build context = repo root) and runs the worker with an injected service-account token, so no API key is committed:

- github_url: "https://github.com/ThomsenDrake/dt-personal-workflows", revision: a commit SHA on "main"
- deployment name: "cn-astroturf-mapper-worker"
- dockerfile path: "deploy/docker/Dockerfile.cn-astroturf-mapper"
- secret: "x_md_api_key" bound to env var `X_MD_API_KEY` (the worker also accepts lowercase `x_md_api_key`) — an x.md key, not an X token — the worker image sets X_MD_BASE_URL to the self-hosted instance, so this key is required there; without it you are limited to ~3 imports/hour per IP on the public instance
- secret: "XAI_API_KEY" (optional; an xAI API key enabling `handle_source` "grok_x_search"/"auto" — never an X token or grok.com login)
- for `handle_source` "grok_cli": the worker image (`deploy/docker/Dockerfile.cn-astroturf-mapper`) now ships the Grok CLI (`npm i -g @xai-official/grok`; `GROK_BIN=grok`, `GROK_HOME=/root/.grok`). No credential is baked in — on the first `grok_cli`/`auto` run with no cached login, the execution pauses on the device-code pending input (see above); complete it once per container lifetime. The workflow never reads or logs the token file
- Via the Studio console (Workflows → Deployments), the API, or the Mistral MCP "deploy_workflow_deployment" tool

Once Active, trigger an execution (this is also what the Vibe skill does):

~~~python
import os
from mistralai.client import Mistral

client = Mistral(api_key=os.environ["MISTRAL_API_KEY"])
execution = client.workflows.execute_workflow(
    workflow_identifier="cn-astroturf-mapper",
    input={"target_handles": ["somehandle", "anotherhandle"], "lookback_days": 90},
    wait_for_result=True,
)
print(execution.model_dump_json(indent=2))
~~~

To ship a new version: push to "main" and redeploy the same deployment name.

## Input schema ("AstroturfMapperInput")

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| "target_handles" | list[str] | **required** | X handles (with or without @) to treat as potential targets |
| "lookback_days" | int | 90 | how far back the handle source walks each handle's posts |
| "max_tweets_per_handle" | int | 500 | cap on posts scanned per handle |
| "with_replies" | bool | false | include the account's replies when resolving posts |
| "with_reposts" | bool | false | never true — reposts are other authors' posts, not the target's own |
| "handle_source" | string | "grok_cli" | "grok_cli" (default: Grok Build CLI headless first, one device-login pause per container, x.md fallback per handle on warning), "xmd" (x.md only), "grok_x_search" (xAI Responses API x_search only), "auto" (x.md first with Grok fallback on warning) |
| "history_scope" | string | "full" | "full" (default: enumerate the handle's whole history, hard cap `max_history_posts`, so the audit covers ALL community-noted tweets) or "recent" (stop at `max_tweets_per_handle`) |
| "max_history_posts" | int | 50000 | hard cap on enumerated posts when `history_scope="full"` |
| "min_notes_for_rater" | int | 3 | raters must have rated ≥ this many target notes to enter cluster analysis |
| "min_co_rating_overlap" | float | 0.5 | Jaccard overlap threshold for linking two raters |
| "min_cluster_size" | int | 5 | cluster must have ≥ this many raters to be flagged |
| "burst_window_minutes" | int | 30 | sliding burst window |
| "burst_min_ratings" | int | 8 | min ratings inside a burst window |
| "burst_one_sided_ratio" | float | 0.8 | min same-direction fraction inside a burst |
| "detect_flips" | bool | true | also scan note-status-history for lost-helpful flips |
| "model" | string | "mistral-large-latest" | model for the LLM campaign assessment |

## Output schema

- "workflow", "params" (echoed input)
- "targets": per handle — resolved user id (if the source returned one), tweet IDs scanned, `source` ("xmd" / "grok_x_search" / "grok_cli" / "auto:..."), optional `x_posts_fetched` (xAI usage), warnings (e.g. unreadable profile); per-handle `user_id` when the source returned one
- "total_ratings_kept": ratings rows kept after filtering to target notes
- "findings": "per_handle" (notes_on_their_posts, noted_tweet_ids — the audited set of noted tweets, clusters with pseudonymous member IDs, bursts, status_flips, coordination_score, severity), "all_clusters", "notes" (with author_participant_id — pseudonymous only)
- "llm_assessment": overall_summary, per-handle verdicts (targeted, severity, evidence, recommended_actions), methodology_caveats
- "report_markdown": full human-readable report
- "disclaimer": pseudonymous-only / hypothesis-not-proof text

## Guardrails (built in)

- Participant IDs are never resolved to handles; the report and the LLM prompt both restate this
- Ratings shards are streamed to a temp file and filtered — the ~184M-row table is never held in memory
- 429s from x.md fail with an actionable message (set X_MD_API_KEY or lower max_tweets_per_handle); 403/404/5xx from x.md only downgrade that handle to a `warning` in the result
- handle_source "grok_x_search": xAI 429 fails with an actionable message; 401/403/5xx or a missing XAI_API_KEY only downgrade that handle to a `warning`; the xAI fetch count is reported per handle as `x_posts_fetched`
- handle_source "grok_cli": missing binary, non-zero exit, or 300s timeout only downgrade that handle to a `warning`; the CLI's credentials are never read or logged by the workflow
- Reposts are always excluded from target post resolution
- Findings are framed as hypotheses; organic partisan brigading is a known false-positive source and is called out in the caveats

## Extending

- **More signals:** add a detector inside "detect_coordination" (e.g. note-creation velocity, cross-target cluster spread) and fold it into "score_handle"
- **Other handle sources:** add a resolver next to `_resolve_handle_xmd` / `_resolve_handle_grok_x_search` / `_resolve_handle_grok_cli` and wire it into "resolve_target_handles" via `handle_source`
- **Reporting:** extend "_render_markdown" or swap the LLM schema ("CampaignAssessment")
