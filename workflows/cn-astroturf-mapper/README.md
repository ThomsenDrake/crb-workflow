# CN Astroturf Mapper (Mistral Studio Workflow)

> This workflow is one self-contained package in the personal Mistral workflows monorepo — see the [repo-root README](../../README.md).

A [Mistral Workflows](https://docs.mistral.ai/studio/workflows/quickstarts/your-first-workflow) project that maps **potential** X Community Notes astroturfing / coordinated-rating campaigns against a given list of X handles (the potential *targets*).

It uses only public data: X's [public Community Notes dataset](https://communitynotes.x.com/guide/en/under-the-hood/download-data) plus the read-only [x.md](https://mdfromx.com/docs) mirror for resolving handles to their recent post IDs.

> **Privacy guardrail (do not remove).** The public dataset exposes raters and note authors only as hashed pseudonymous `participantId`s. This workflow **never** maps participantIds back to X handles or accounts — it reports coordination *clusters of pseudonymous IDs*, never identities. Re-identification would violate X's data terms; attribution requires external data and is out of scope. Findings are hypotheses for human review, not proof of a campaign.

## Pipeline

~~~mermaid
flowchart LR
    A[resolve_target_handles<br/>x.md: handles -> recent tweet IDs] --> B[load_target_notes<br/>CN notes attached to those tweets]
    B --> C[load_ratings_and_status<br/>CN ratings + status history, streamed]
    C --> D[detect_coordination<br/>co-rating clusters, bursts, status flips, per-handle score]
    D --> E[llm_campaign_assessment<br/>structured narrative per handle]
    E --> F[report_markdown]
~~~

All steps are `@workflows.activity()` functions (durable + retried, so an execution resumes after a worker restart); the `@workflows.workflow.define(name="cn-astroturf-mapper")` entrypoint stays deterministic. The `get_steps` query exposes live per-step progress.

## Data sources

- **x.md** (`https://mdfromx.com/docs`) — keyless read-only mirror of public X content. Bulk history route: `GET https://mdfromx.com/api/v1/profiles/{handle}/posts?since=YYYY-MM-DD&max_posts=N&format=json&with_replies=..&with_reposts=false`. Each post's `id` is the numeric tweet ID — exactly what joins to the CN dataset's `tweetId`. `meta.truncated` / `meta.next_until` paginate the walk; 429s fail with an actionable error, while 403/404/5xx responses only downgrade that handle to a per-handle `warning` so one bad profile doesn't sink the whole run. An optional `X_MD_API_KEY` (sent as `Authorization: Bearer`) raises the import allowance (without a key: ~3 imports/hour, 5/day per IP). It is **not** an X API token.
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
- secret: "X_MD_API_KEY" (optional; an x.md key, not an X token) — without it you are limited to ~3 imports/hour per IP
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
| "lookback_days" | int | 90 | how far back x.md walks each handle's posts |
| "max_tweets_per_handle" | int | 500 | cap on posts scanned per handle |
| "with_replies" | bool | false | include the account's replies when resolving posts |
| "with_reposts" | bool | false | never true — reposts are other authors' posts, not the target's own |
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
- "targets": per handle — resolved user id (if x.md returned one), tweet IDs scanned, warnings (e.g. unreadable profile)
- "total_ratings_kept": ratings rows kept after filtering to target notes
- "findings": "per_handle" (notes_on_their_posts, clusters with pseudonymous member IDs, bursts, status_flips, coordination_score, severity), "all_clusters", "notes" (with author_participant_id — pseudonymous only)
- "llm_assessment": overall_summary, per-handle verdicts (targeted, severity, evidence, recommended_actions), methodology_caveats
- "report_markdown": full human-readable report
- "disclaimer": pseudonymous-only / hypothesis-not-proof text

## Guardrails (built in)

- Participant IDs are never resolved to handles; the report and the LLM prompt both restate this
- Ratings shards are streamed to a temp file and filtered — the ~184M-row table is never held in memory
- 429s from x.md fail with an actionable message (set X_MD_API_KEY or lower max_tweets_per_handle); 403/404/5xx from x.md only downgrade that handle to a `warning` in the result
- Reposts are always excluded from target post resolution
- Findings are framed as hypotheses; organic partisan brigading is a known false-positive source and is called out in the caveats

## Extending

- **More signals:** add a detector inside "detect_coordination" (e.g. note-creation velocity, cross-target cluster spread) and fold it into "score_handle"
- **Other handle sources:** add a fetch_\<name\> alongside the x.md route in "resolve_target_handles"
- **Reporting:** extend "_render_markdown" or swap the LLM schema ("CampaignAssessment")
