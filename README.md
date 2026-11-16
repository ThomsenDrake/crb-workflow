# Personal Mistral Workflows (monorepo)

Monorepo for my personal [Mistral Workflows](https://docs.mistral.ai/studio/workflows/quickstarts/your-first-workflow) projects. Each workflow is a self-contained package under `workflows/<name>/` with its worker app, examples, Vibe skill, and README. Shared deployment assets and env config live at the repo root.

## Workflows

| Package | Workflow identifier | Deployment | Description |
| --- | --- | --- | --- |
| [workflows/ncaa-d1-best-line](workflows/ncaa-d1-best-line/README.md) | `ncaa-d1-best-line` | `crb-workflow-worker` | NCAA D1 (FBS) best-line finder: live ESPN odds ingestion, line shopping across books, edge/EV vs model projections, fractional-Kelly staking, Markdown report |

## Layout

~~~
workflows/<name>/
  worker/       # Mistral Workflows worker app (pyproject.toml, src/, tests/)
  examples/     # run_execution.py + sample inputs
  skill/        # Vibe slash-command skill (SKILL.md) that invokes the deployed workflow
  README.md     # workflow-specific docs
deploy/docker/
  Dockerfile.<name>   # one managed-deployment image per workflow (build context = repo root)
.env.example    # shared env template (MISTRAL_API_KEY + optional source keys)
~~~

## Adding a new workflow

1. `mkdir -p workflows/<name>/{worker,examples,skill}` and copy `workflows/ncaa-d1-best-line/worker` as the starting worker app (rename the package in `pyproject.toml`).
2. Add `deploy/docker/Dockerfile.<name>` (copy `Dockerfile.worker`, point the `COPY` lines at `workflows/<name>/worker`).
3. Create a managed deployment (Studio console, API, or the Mistral MCP `deploy_workflow_deployment` tool) with `github_url` = this repo, a commit SHA on `main`, and dockerfile path `deploy/docker/Dockerfile.<name>`; reuse the deployment name to update it.
4. Document the workflow in `workflows/<name>/README.md` and add a row to the table above.

## Local dev

- `cp .env.example .env` and set `MISTRAL_API_KEY`.
- From the repo root, `mistral dev` auto-discovers every worker under `workflows/`; or run one directly, e.g. `cd workflows/ncaa-d1-best-line/worker && uv sync && PYTHONPATH=src uv run python -m worker.entrypoints.worker`.
- Trigger an execution, e.g. `python workflows/ncaa-d1-best-line/examples/run_execution.py`.

## Disclaimer

These projects are for entertainment and analysis only, do not provide betting advice, and guarantee no outcomes. Only wager what you can afford to lose, in jurisdictions where it is legal, 21+. If gambling is a problem, call or text 1-800-GAMBLER (US).
