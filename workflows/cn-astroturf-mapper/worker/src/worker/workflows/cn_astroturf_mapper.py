"""
cn-astroturf-mapper — Mistral Workflow (Studio, Temporal-backed)

Maps POTENTIAL Community Notes astroturfing campaigns against a given list of
X handles (the potential *targets*), using only X's public Community Notes
dataset (https://communitynotes.x.com/guide/en/under-the-hood/download-data).

PRIVACY GUARDRAIL (do not remove)
---------------------------------
The public dataset exposes raters/writers only as hashed pseudonymous
`participantId`s. This workflow MUST NOT map participantIds back to X
handles/accounts. It reports coordination *clusters of pseudonymous IDs*,
never identities. Re-identification would violate X's data terms and privacy
law; attribution requires external data and is out of scope here.

Pipeline (each step is an @workflows.activity so all side effects are durable
and retried; the entrypoint stays deterministic):

  1. resolve_target_handles  handles -> recent post IDs via x.md
                             (https://mdfromx.com/docs, GET /api/v1/profiles/{handle}/posts;
                             plain GET, no X credentials), or — when input handle_source is
                             "grok_x_search"/"grok_cli"/"auto" — via Grok X Search: the xAI
                             Responses API x_search tool (XAI_API_KEY; billed per post
                             fetched) or the Grok Build CLI in headless mode (its own cached
                             login; headless -p is documented for scripts/CI)
  2. load_target_notes       CN public data: notes attached to those tweets
  3. load_ratings_and_status CN public data: ratings + status history for those
                             notes (streamed + filtered, the ratings table is
                             ~184M rows so it is never loaded wholesale)
  4. detect_coordination     pure analytics: co-rating clusters (union-find on
                             Jaccard overlap), rating bursts in time windows,
                             post-display status flips, per-handle score
  5. llm_campaign_assessment Mistral model writes the per-handle narrative

Deploy: this module lives in the personal-workflows monorepo at
workflows/cn-astroturf-mapper/worker/src/worker/workflows/cn_astroturf_mapper.py
(deps: "mistralai-workflows[mistralai]>=3.10.0,<4", "pydantic", "httpx"; image:
deploy/docker/Dockerfile.cn-astroturf-mapper, build context = repo root) and
runs on its own managed deployment, "cn-astroturf-mapper-worker":

  execute_workflow(
      workflow_identifier="cn-astroturf-mapper",
      deployment_name="cn-astroturf-mapper-worker",
      input={
          "target_handles": ["somehandle", "anotherhandle"],
          "lookback_days": 90,
      },
  )

Env vars:
  X_MD_API_KEY    optional x.md API key (Authorization: Bearer). Without it the
                  bulk import is limited to ~3 imports/hour, 5/day per IP; with a
                  key you get your key's import allowance. Never an X token.
  XAI_API_KEY     optional xAI API key enabling the grok_x_search handle source
                  (xAI Responses API x_search tool; billed per post fetched — an
                  xAI meter, not X's pay-per-use API). Never an X token or a
                  grok.com login.
  XAI_API_BASE    xAI API base URL (default https://api.x.ai/v1)
  XAI_XSEARCH_MODEL  model used for x_search calls (default grok-4.7)
  GROK_BIN        grok binary for the grok_cli source (default "grok" on PATH;
                  ~/.grok/bin/grok per xAI docs). GROK_HOME may point the CLI at its
                  credential dir; the cached login (~/.grok/auth.json) is used as-is
                  and is never read, printed, or logged by this module.
  CN_DATA_BASE      base URL of public CN snapshots
                    (default https://ton.twimg.com/birdwatch-public-data)
  CN_LOOKBACK_DAYS  days to walk back from today to find the latest daily
                    snapshot (default 7)
  CN_MAX_SHARDS     max shard index probed per table (default 64)
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import os
import re
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field

import mistralai.workflows as workflows
import mistralai.workflows.plugins.mistralai as workflows_mistralai

# ── Input / output models ─────────────────────────────────────────────────────


class AstroturfMapperInput(BaseModel):
    target_handles: list[str] = Field(description="X handles (with or without @) to treat as potential astroturfing targets")
    lookback_days: int = 90
    max_tweets_per_handle: int = 500
    with_replies: bool = False   # include the account's replies when resolving posts (via x.md)
    with_reposts: bool = False   # never true: reposts are other people's posts, not targets' own
    handle_source: Literal["xmd", "grok_x_search", "grok_cli", "auto"] = "xmd"  # xmd = x.md only; grok_x_search = xAI Responses API x_search only; grok_cli = Grok Build CLI headless only; auto = x.md with per-handle grok fallback on warning
    min_notes_for_rater: int = 3          # rater must have rated >= this many target notes to enter cluster analysis
    min_co_rating_overlap: float = 0.5    # Jaccard overlap threshold for linking two raters
    min_cluster_size: int = 5             # cluster must have >= this many raters to be flagged
    burst_window_minutes: int = 30
    burst_min_ratings: int = 8
    burst_one_sided_ratio: float = 0.8    # fraction of same-direction ratings inside a burst window
    detect_flips: bool = True
    model: str = "mistral-large-latest"


class HandleAssessment(BaseModel):
    handle: str
    targeted: bool
    notes_on_their_posts: int
    coordination_score: float
    severity: str
    summary: str
    evidence: list[str]
    recommended_actions: list[str]


class CampaignAssessment(BaseModel):
    overall_summary: str
    per_handle: list[HandleAssessment]
    methodology_caveats: list[str]


# ── Small pure helpers (also used inside activities) ──────────────────────────

HELPFUL_VALUES = {"HELPFUL", "SOMEWHAT_HELPFUL"}


def _jaccard(a: set, b: set) -> float:
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)


class _UnionFind:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def find_corating_clusters(rater_notes: dict, min_overlap: float, min_cluster_size: int, min_notes: int):
    """Cluster raters whose rated-note sets overlap heavily (coordination signature)."""
    raters = [r for r, notes in rater_notes.items() if len(notes) >= min_notes]
    if not raters:
        return []
    uf = _UnionFind(raters)
    note_raters = defaultdict(list)
    for r in raters:
        for n in rater_notes[r]:
            note_raters[n].append(r)
    candidate_pairs = set()
    for rs in note_raters.values():
        for i in range(len(rs)):
            for j in range(i + 1, len(rs)):
                candidate_pairs.add((rs[i], rs[j]))
    for a, b in candidate_pairs:
        if _jaccard(rater_notes[a], rater_notes[b]) >= min_overlap:
            uf.union(a, b)
    grouped = defaultdict(list)
    for r in raters:
        grouped[uf.find(r)].append(r)
    clusters = []
    for members in grouped.values():
        if len(members) < min_cluster_size:
            continue
        notes = set().union(*(rater_notes[m] for m in members))
        overlaps = [
            _jaccard(rater_notes[a], rater_notes[b])
            for i, a in enumerate(members)
            for b in members[i + 1:]
        ]
        digest = hashlib.sha1("|".join(sorted(members)).encode()).hexdigest()[:10]
        clusters.append({
            "cluster_id": f"C-{digest}",
            "size": len(members),
            "member_participant_ids": sorted(members),  # pseudonymous ONLY — never resolve to handles
            "notes_covered": sorted(notes),
            "mean_pairwise_overlap": round(sum(overlaps) / len(overlaps), 4) if overlaps else 0.0,
        })
    clusters.sort(key=lambda c: (-c["size"], -c["mean_pairwise_overlap"]))
    return clusters


def detect_bursts(ratings_by_note: dict, window_ms: int, min_ratings: int, one_sided_ratio: float):
    """Sliding-window bursts of same-direction ratings on a single note."""
    bursts = []
    for note_id, ratings in ratings_by_note.items():
        rs = sorted(ratings, key=lambda r: r["createdAtMillis"])
        best = None
        j = 0
        for i, r in enumerate(rs):
            while j < i and rs[j]["createdAtMillis"] < r["createdAtMillis"] - window_ms:
                j += 1
            window = rs[j:i + 1]
            if len(window) < min_ratings:
                continue
            helpful = sum(1 for x in window if x["helpfulness"] in HELPFUL_VALUES)
            not_helpful = sum(1 for x in window if x["helpfulness"] == "NOT_HELPFUL")
            dominant, direction = max((helpful, "helpful"), (not_helpful, "not_helpful"))
            ratio = dominant / len(window)
            if ratio >= one_sided_ratio and (best is None or len(window) > best["count"]):
                best = {
                    "count": len(window),
                    "one_sided_ratio": round(ratio, 3),
                    "direction": direction,
                    "window_start_ms": window[0]["createdAtMillis"],
                    "window_end_ms": r["createdAtMillis"],
                }
        if best:
            bursts.append({"noteId": note_id, **best})
    return bursts


def detect_status_flips(status_by_note: dict):
    """Notes that reached CURRENTLY_RATED_HELPFUL and later lost it (post-display downvote raids)."""
    flips = []
    for note_id, hist in status_by_note.items():
        hs = sorted(hist, key=lambda h: h["createdAtMillis"])
        seen_helpful = False
        for h in hs:
            status = (h.get("status") or "").upper()
            if status == "CURRENTLY_RATED_HELPFUL":
                seen_helpful = True
            elif seen_helpful and status and status != "CURRENTLY_RATED_HELPFUL":
                flips.append({
                    "noteId": note_id,
                    "lost_helpful_status_at_ms": h["createdAtMillis"],
                    "new_status": status,
                })
                seen_helpful = False
    return flips


def score_handle(n_notes: int, clusters: list, bursts: list, flips: list) -> tuple:
    if n_notes == 0:
        return 0.0, "none"
    max_cluster = max((c["size"] for c in clusters), default=0)
    score = (
        min(40.0, 10.0 * len(clusters))
        + min(25.0, 2.5 * max_cluster)
        + min(20.0, 4.0 * len(bursts))
        + min(15.0, 5.0 * len(flips))
    )
    score = round(min(score, 100.0), 1)
    severity = (
        "critical" if score >= 75 else
        "high" if score >= 50 else
        "medium" if score >= 25 else "low"
    )
    return score, severity


# ── Activity 1: resolve target handles to their recent posts via x.md ────────
#
# x.md (https://mdfromx.com/docs) is a read-only, keyless mirror of public X
# content. Bulk history route:
#   GET https://mdfromx.com/api/v1/profiles/{handle}/posts
#       ?since=YYYY-MM-DD&max_posts=N&format=json&with_replies=..&with_reposts=false
# Response: {"profile": {...}, "posts": [{"id": "<tweet id>", "text": ...,
# "created_at": ..., "url": "https://x.com/.../status/<id>", ...}],
# "meta": {"count", "truncated", "next_until", ...}}
# Post `id` is the numeric tweet ID — exactly what joins to the CN dataset's
# `tweetId` column. Optional X_MD_API_KEY raises the import rate allowance.


XAI_API_BASE = os.environ.get("XAI_API_BASE", "https://api.x.ai/v1")
XAI_XSEARCH_MODEL = os.environ.get("XAI_XSEARCH_MODEL", "grok-4.7")

_STATUS_URL_RE = re.compile(r"(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,20})/status(?:es)?/(\d+)", re.IGNORECASE)


def _extract_status_ids(text: str, handle: Optional[str] = None) -> list[str]:
    """Numeric tweet IDs from x.com status URLs in `text`, deduped in order, optionally restricted to one handle."""
    seen = set()
    out = []
    for m in _STATUS_URL_RE.finditer(text or ""):
        if handle and m.group(1).lower() != handle.lower():
            continue
        sid = m.group(2)
        if sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


async def _resolve_handle_xmd(client, handle: str, since: str, p: AstroturfMapperInput) -> dict:
    """x.md bulk-history route for one handle (see module docs). Degrades to a per-handle warning on 403/404/5xx."""
    seen = set()
    tweet_ids = []
    until = None
    body = {}
    warning = None
    while len(tweet_ids) < p.max_tweets_per_handle:
        params = {
            "since": since,
            "max_posts": min(5000, p.max_tweets_per_handle - len(tweet_ids) + 1),
            "format": "json",
            "with_replies": "true" if p.with_replies else "false",
            "with_reposts": "false",  # reposts are the ORIGINAL author's post — exclude
        }
        if until:
            params["until"] = until
        r = await client.get(f"/api/v1/profiles/{handle}/posts", params=params)
        if r.status_code == 429:
            raise RuntimeError(
                f"x.md rate limited on @{handle} (Retry-After: {r.headers.get('Retry-After', '?')}s); "
                "set X_MD_API_KEY for a higher import allowance or reduce max_tweets_per_handle"
            )
        if r.status_code in (403, 404):
            warning = "profile not found or not publicly readable via x.md"
            break
        if r.status_code >= 500:
            # x.md bulk-import occasionally 500s (e.g. very large profiles); degrade per-handle
            warning = f"x.md returned HTTP {r.status_code} for @{handle}; no posts resolved for this handle"
            break
        r.raise_for_status()
        try:
            body = r.json()
        except ValueError:
            warning = f"x.md returned a non-JSON response for @{handle}; no posts resolved for this handle"
            break
        for post in body.get("posts", []):
            pid = str(post.get("id") or "")
            if pid and pid not in seen:
                seen.add(pid)
                tweet_ids.append(pid)
        meta = body.get("meta", {})
        if meta.get("truncated") and meta.get("next_until") and len(tweet_ids) < p.max_tweets_per_handle:
            until = meta["next_until"]  # continue the walk; dedupe boundary via `seen`
        else:
            break
    entry = {
        "handle": handle,
        "user_id": (body.get("profile") or {}).get("id") if body.get("profile") else None,
        "tweet_ids": tweet_ids[: p.max_tweets_per_handle],
        "tweets_scanned": len(tweet_ids[: p.max_tweets_per_handle]),
        "source": "xmd",
    }
    if warning:
        entry["warning"] = warning
    return entry


async def _resolve_handle_grok_x_search(handle: str, since: str, until: str, p: AstroturfMapperInput) -> dict:
    """Resolve one handle to recent post IDs via xAI's Grok X Search tool.

    Published xAI Responses API route (not a grok.com/X login): x_search scoped with
    allowed_x_handles + from_date/to_date; the model echoes the x.com post URLs and the
    numeric tweet IDs are parsed out of them. Billed per post fetched ($5/1k posts as of
    2026-09, reported in usage.server_side_tool_usage_details) plus tokens — an xAI
    meter, not X's pay-per-use API.
    """
    import httpx

    entry = {"handle": handle, "user_id": None, "tweet_ids": [], "tweets_scanned": 0, "source": "grok_x_search"}
    api_key = os.environ.get("XAI_API_KEY")
    if not api_key:
        entry["warning"] = "XAI_API_KEY not set; grok_x_search source unavailable for this handle"
        return entry
    query = f"from:{handle} -is:retweet" + ("" if p.with_replies else " -is:reply")
    prompt = (
        f"Use X Search only (no web search). List up to {p.max_tweets_per_handle} original posts by "
        f"@{handle} posted from {since} through {until} (query: {query}). Reply with ONLY the full x.com "
        "post URLs (https://x.com/<handle>/status/<id>), one per line, most recent first. This is "
        "read-only: do not post, reply, like, repost, or follow anything. If no posts are found, reply exactly: NONE"
    )
    body = {
        "model": XAI_XSEARCH_MODEL,
        "input": [{"role": "user", "content": prompt}],
        "tools": [{"type": "x_search", "allowed_x_handles": [handle], "from_date": since, "to_date": until}],
        "max_turns": 2,
        "stream": False,
    }
    async with httpx.AsyncClient(timeout=300.0) as client:
        r = await client.post(
            f"{XAI_API_BASE}/responses",
            json=body,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
    if r.status_code == 429:
        raise RuntimeError(
            f"xAI rate limited on grok x_search for @{handle} (Retry-After: {r.headers.get('Retry-After', '?')}s); "
            "reduce max_tweets_per_handle or check the XAI team rate limits"
        )
    if r.status_code in (401, 403):
        entry["warning"] = "XAI_API_KEY missing or invalid; grok_x_search unavailable for this handle"
        return entry
    if r.status_code >= 500:
        entry["warning"] = f"xAI returned HTTP {r.status_code} for grok x_search on @{handle}"
        return entry
    r.raise_for_status()
    resp = r.json()
    texts = []
    for item in resp.get("output", []):
        if item.get("type") != "message":
            continue
        for c in item.get("content", []):
            if c.get("type") == "output_text":
                texts.append(c.get("text") or "")
                for a in c.get("annotations") or []:
                    if a.get("url"):
                        texts.append(a["url"])
    ids = _extract_status_ids("\n".join(texts), handle)
    usage = (resp.get("usage") or {}).get("server_side_tool_usage_details") or {}
    entry["tweet_ids"] = ids[: p.max_tweets_per_handle]
    entry["tweets_scanned"] = len(entry["tweet_ids"])
    if usage.get("x_posts_fetched"):
        entry["x_posts_fetched"] = usage["x_posts_fetched"]
    if not ids:
        entry["warning"] = f"grok x_search found no posts for @{handle} in the requested window"
    return entry


async def _resolve_handle_grok_cli(handle: str, since: str, until: str, p: AstroturfMapperInput) -> dict:
    """Resolve one handle via the Grok Build CLI in headless mode (documented for scripts/CI).

    Runs `grok -p` with a read-only, X-Search-only prompt and parses tweet IDs from the
    echoed x.com status URLs. Auth is the CLI's own cached login (GROK_HOME /
    ~/.grok/auth.json) or XAI_API_KEY — this code never reads, prints, or passes
    credentials. Requires the grok binary (GROK_BIN, default "grok" on PATH).
    """
    import shutil
    import subprocess

    entry = {"handle": handle, "user_id": None, "tweet_ids": [], "tweets_scanned": 0, "source": "grok_cli"}
    bin_path = os.environ.get("GROK_BIN", "grok")
    if shutil.which(bin_path) is None and not os.path.exists(bin_path):
        entry["warning"] = f"grok CLI not found ({bin_path}); grok_cli source unavailable for this handle"
        return entry
    query = f"from:{handle} -is:retweet" + ("" if p.with_replies else " -is:reply")
    prompt = (
        f"Use X Search (x_search) only, skip web search. List up to {p.max_tweets_per_handle} original "
        f"posts by @{handle} posted from {since} through {until} (query: {query}). Reply with ONLY the "
        "full x.com post URLs (https://x.com/<handle>/status/<id>), one per line, most recent first. "
        "This task is read-only: do not post, reply, like, repost, or follow anything. "
        "If no posts are found, reply exactly: NONE"
    )
    cmd = [bin_path, "--no-subagents", "--no-auto-update", "--max-turns", "6", "--reasoning-effort", "low", "-p", prompt]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=tempfile.mkdtemp(),
            env=os.environ.copy(),
        )
    except OSError as e:
        entry["warning"] = f"grok CLI failed to start for @{handle}: {e}"
        return entry
    try:
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=300)
    except asyncio.TimeoutError:
        proc.kill()
        entry["warning"] = f"grok CLI timed out after 300s on @{handle}"
        return entry
    if proc.returncode != 0:
        tail = err_b.decode(errors="replace")[-300:]
        entry["warning"] = f"grok CLI exited {proc.returncode} for @{handle} (not signed in? run `grok login --device-code`): {tail}"
        return entry
    ids = _extract_status_ids(out_b.decode(errors="replace"), handle)
    entry["tweet_ids"] = ids[: p.max_tweets_per_handle]
    entry["tweets_scanned"] = len(entry["tweet_ids"])
    if not ids:
        entry["warning"] = f"grok CLI found no posts for @{handle} in the requested window"
    return entry


async def _grok_fallbacks(handle: str, since: str, until: str, p: AstroturfMapperInput) -> list[dict]:
    """Candidate grok resolvers for handle_source="auto", in order: xAI API if keyed, then the CLI if installed."""
    import shutil

    out = []
    if os.environ.get("XAI_API_KEY"):
        g = await _resolve_handle_grok_x_search(handle, since, until, p)
        out.append(g)
        if g.get("tweet_ids"):
            return out
    bin_path = os.environ.get("GROK_BIN", "grok")
    if shutil.which(bin_path) is not None or os.path.exists(bin_path):
        out.append(await _resolve_handle_grok_cli(handle, since, until, p))
    return out


@workflows.activity(start_to_close_timeout=timedelta(minutes=15), retry_policy_max_attempts=3)
async def resolve_target_handles(p: AstroturfMapperInput) -> list[dict]:
    import httpx

    since = (datetime.now(timezone.utc) - timedelta(days=p.lookback_days)).strftime("%Y-%m-%d")
    until = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")  # x_search to_date is exclusive
    out = []
    if p.handle_source in ("grok_x_search", "grok_cli"):
        resolver = _resolve_handle_grok_x_search if p.handle_source == "grok_x_search" else _resolve_handle_grok_cli
        for raw in p.target_handles:
            out.append(await resolver(raw.lstrip("@"), since, until, p))
        return out
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("X_MD_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    async with httpx.AsyncClient(base_url="https://mdfromx.com", headers=headers, timeout=300.0) as client:
        for raw in p.target_handles:
            handle = raw.lstrip("@")
            entry = await _resolve_handle_xmd(client, handle, since, p)
            if p.handle_source == "auto" and entry.get("warning"):
                note = entry["warning"]
                for g in await _grok_fallbacks(handle, since, until, p):
                    if g.get("tweet_ids"):
                        entry["tweet_ids"] = g["tweet_ids"]
                        entry["tweets_scanned"] = g["tweets_scanned"]
                        entry["source"] = f"auto:xmd-warning+{g['source']}"
                        entry["warning"] = f"x.md: {note}; recovered via {g['source']}"
                        if g.get("x_posts_fetched"):
                            entry["x_posts_fetched"] = g["x_posts_fetched"]
                        break
                    if g.get("warning"):
                        note = f"{note}; {g['source']}: {g['warning']}"
                entry["warning"] = note
            out.append(entry)
    return out


# ── Activities 2-3: pull the public CN dataset, streamed + filtered ──────────

CN_DATA_BASE = os.environ.get("CN_DATA_BASE", "https://ton.twimg.com/birdwatch-public-data")
CN_MAX_SHARDS = int(os.environ.get("CN_MAX_SHARDS", "64"))
CN_LOOKBACK_DAYS = int(os.environ.get("CN_LOOKBACK_DAYS", "7"))  # snapshots are daily best-effort; walk back if today's is missing
# table -> candidate (dir, prefix) pairs; X has used both camelCase and kebab-case dir names
CN_TABLES = {
    "notes": [("notes", "notes")],
    "ratings": [("noteRatings", "ratings"), ("ratings", "ratings")],
    "status": [("noteStatusHistory", "noteStatusHistory"), ("note-status-history", "note-status-history")],
}


async def _shard_urls(client: httpx.AsyncClient, candidates: list) -> list[str]:
    """Resolve the current snapshot's shard URLs for a table.

    Layout (as of 2026): {CN_DATA_BASE}/YYYY/MM/DD/<table-dir>/<prefix>-NNNNN.zip,
    one snapshot per day. Walk back up to CN_LOOKBACK_DAYS from today until shard
    00000 exists, then enumerate contiguous shards. Falls back to the legacy
    undated layout, then to [] (caller treats that as "no data").
    """
    import httpx

    today = datetime.now(timezone.utc).date()
    date_dirs = [(today - timedelta(days=i)).strftime("%Y/%m/%d") for i in range(CN_LOOKBACK_DAYS)]
    date_dirs.append("")  # legacy undated layout
    for table_dir, prefix in candidates:
        for dd in date_dirs:
            base = f"{CN_DATA_BASE}/{dd}" if dd else CN_DATA_BASE
            first = f"{base}/{table_dir}/{prefix}-00000.zip"
            try:
                r = await client.head(first, follow_redirects=True)
                if r.status_code != 200:
                    continue
            except httpx.HTTPError:
                continue
            urls = [first]
            for i in range(1, CN_MAX_SHARDS):
                url = f"{base}/{table_dir}/{prefix}-{i:05d}.zip"
                try:
                    r = await client.head(url, follow_redirects=True)
                except httpx.HTTPError:
                    break
                if r.status_code == 200:
                    urls.append(url)
                else:
                    break  # shards are contiguous from 00000
            return urls
        # legacy unsharded single file
        single = f"{CN_DATA_BASE}/{table_dir}/{prefix}.zip"
        try:
            r = await client.head(single, follow_redirects=True)
            if r.status_code == 200:
                return [single]
        except httpx.HTTPError:
            pass
    return []


async def _aiter_tsv_rows(client: httpx.AsyncClient, url: str):
    """Async generator streaming a zipped TSV shard row-by-row (dict rows keyed by header).

    The ratings table is ~184M rows across many shards, so shards are streamed to a
    temp file and parsed incrementally — never loaded into memory wholesale.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp_path = tmp.name
    tmp.close()
    try:
        async with client.stream("GET", url, follow_redirects=True, timeout=600.0) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=1 << 20):
                    f.write(chunk)
        with zipfile.ZipFile(tmp_path) as zf:
            name = next(n for n in zf.namelist() if n.endswith(".tsv"))
            with zf.open(name) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8", newline="")
                reader = csv.DictReader(text, delimiter="\t")
                for row in reader:
                    yield row
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


@workflows.activity(start_to_close_timeout=timedelta(hours=2), retry_policy_max_attempts=1)
async def load_target_notes(target_tweet_ids: set) -> list[dict]:
    """Notes attached to any target tweet."""
    import httpx

    async with httpx.AsyncClient(timeout=120.0) as client:
        urls = await _shard_urls(client, CN_TABLES["notes"])
        notes = []
        for url in urls:
            async for row in _aiter_tsv_rows(client, url):
                if row.get("tweetId") in target_tweet_ids:
                    notes.append({
                        "noteId": row.get("noteId"),
                        "tweetId": row.get("tweetId"),
                        "author_participant_id": row.get("participantId"),  # pseudonymous only
                        "createdAtMillis": int(row.get("createdAtMillis") or 0),
                        "classification": row.get("classification"),
                        "summary_excerpt": (row.get("summary") or "")[:300],
                    })
    return notes


@workflows.activity(start_to_close_timeout=timedelta(hours=3), retry_policy_max_attempts=1)
async def load_ratings_and_status(target_note_ids: set, detect_flips: bool) -> dict:
    """Ratings + note status history restricted to target notes (streamed)."""
    import httpx

    ratings_by_note = defaultdict(list)
    status_by_note = defaultdict(list)
    async with httpx.AsyncClient(timeout=120.0) as client:
        # ratings
        for url in await _shard_urls(client, CN_TABLES["ratings"]):
            async for row in _aiter_tsv_rows(client, url):
                nid = row.get("noteId")
                if nid in target_note_ids:
                    ratings_by_note[nid].append({
                        "noteId": nid,
                        "participantId": row.get("participantId"),  # pseudonymous only
                        "createdAtMillis": int(row.get("createdAtMillis") or 0),
                        "helpfulness": (row.get("helpfulnessLevel") or "").upper(),
                    })
        # status history
        if detect_flips:
            for url in await _shard_urls(client, CN_TABLES["status"]):
                async for row in _aiter_tsv_rows(client, url):
                    nid = row.get("noteId")
                    if nid in target_note_ids:
                        status = row.get("currentStatus") or row.get("mostRecentNonNMRStatus") or ""
                        ts = row.get("timestampMillisOfCurrentStatus") or row.get("createdAtMillis") or 0
                        status_by_note[nid].append({
                            "noteId": nid,
                            "createdAtMillis": int(ts or 0),
                            "status": (status or "").upper(),
                        })
    return {
        "ratings_by_note": {k: v for k, v in ratings_by_note.items()},
        "status_by_note": {k: v for k, v in status_by_note.items()},
        "total_ratings_kept": sum(len(v) for v in ratings_by_note.values()),
    }


# ── Activity 4: coordination analytics (pure, deterministic) ──────────────────


@workflows.activity(start_to_close_timeout=timedelta(minutes=30), retry_policy_max_attempts=2)
async def detect_coordination(targets: list, notes: list, data: dict, p: AstroturfMapperInput) -> dict:
    tweet_to_handle = {}
    for t in targets:
        for tid in t["tweet_ids"]:
            tweet_to_handle[tid] = t["handle"]
    note_handle = {n["noteId"]: tweet_to_handle.get(n["tweetId"], "unknown") for n in notes}

    ratings_by_note = data["ratings_by_note"]
    status_by_note = data.get("status_by_note", {})

    # rater -> target notes they rated
    rater_notes = defaultdict(set)
    for nid, rs in ratings_by_note.items():
        for r in rs:
            rater_notes[r["participantId"]].add(nid)

    clusters = find_corating_clusters(
        rater_notes,
        min_overlap=p.min_co_rating_overlap,
        min_cluster_size=p.min_cluster_size,
        min_notes=p.min_notes_for_rater,
    )
    cluster_notes = {nid for c in clusters for nid in c["notes_covered"]}

    bursts = detect_bursts(
        ratings_by_note,
        window_ms=p.burst_window_minutes * 60 * 1000,
        min_ratings=p.burst_min_ratings,
        one_sided_ratio=p.burst_one_sided_ratio,
    )
    flips = detect_status_flips(status_by_note) if p.detect_flips else []

    # attribute clusters/bursts/flips to handles via their notes
    handle_notes = defaultdict(list)
    for n in notes:
        handle_notes[note_handle[n["noteId"]]].append(n)

    per_handle = {}
    for t in targets:
        h = t["handle"]
        h_notes = handle_notes.get(h, [])
        h_note_ids = {n["noteId"] for n in h_notes}
        h_clusters = [
            {**c, "handles_covered": sorted({note_handle[n] for n in c["notes_covered"]})}
            for c in clusters if set(c["notes_covered"]) & h_note_ids
        ]
        h_bursts = [b for b in bursts if b["noteId"] in h_note_ids]
        h_flips = [f for f in flips if f["noteId"] in h_note_ids]
        score, severity = score_handle(len(h_notes), h_clusters, h_bursts, h_flips)
        per_handle[h] = {
            "handle": h,
            "tweets_scanned": t["tweets_scanned"],
            "notes_on_their_posts": len(h_notes),
            "note_ids": sorted(h_note_ids),
            "clusters": h_clusters,
            "bursts": h_bursts,
            "status_flips": h_flips,
            "coordination_score": score,
            "severity": severity,
        }

    return {
        "per_handle": per_handle,
        "all_clusters": clusters,
        "cluster_note_ids": sorted(cluster_notes),
        "bursts": bursts,
        "flips": flips,
        "notes": [
            {k: n[k] for k in ("noteId", "tweetId", "author_participant_id", "createdAtMillis", "classification", "summary_excerpt")}
            for n in notes
        ],
    }


# ── Activity 5: LLM narrative assessment ──────────────────────────────────────


@workflows.activity(start_to_close_timeout=timedelta(minutes=5), retry_policy_max_attempts=2)
async def llm_campaign_assessment(findings: dict, p: AstroturfMapperInput) -> dict:
    client = workflows_mistralai.get_mistral_client()
    compact = {
        "per_handle": {
            h: {
                "tweets_scanned": v["tweets_scanned"],
                "notes_on_their_posts": v["notes_on_their_posts"],
                "coordination_score": v["coordination_score"],
                "severity": v["severity"],
                "n_clusters": len(v["clusters"]),
                "cluster_sizes": [c["size"] for c in v["clusters"]],
                "cluster_overlaps": [c["mean_pairwise_overlap"] for c in v["clusters"]],
                "n_bursts": len(v["bursts"]),
                "burst_details": v["bursts"][:10],
                "n_status_flips": len(v["status_flips"]),
            }
            for h, v in findings["per_handle"].items()
        },
        "total_clusters": len(findings["all_clusters"]),
    }
    prompt = (
        "You are a trust & safety analyst. Below is the output of an automated detector that maps "
        "potential astroturfing/coordination campaigns in X Community Notes ratings against a list of "
        "target handles. Rater identities are pseudonymous participant IDs only; DO NOT speculate about "
        "or attempt real identities.\n\n"
        "Signals: co-rating clusters (groups of raters rating the same target notes with high Jaccard "
        "overlap), one-sided rating bursts in short time windows, and notes losing 'Currently Rated "
        "Helpful' status after display.\n\n"
        "Produce a structured assessment: per-handle verdict (targeted or not), coordination score "
        "interpretation, evidence bullets, recommended actions (e.g. document, report to X, monitor), "
        "and methodology caveats (false-positive sources: organic partisan brigading vs true coordination, "
        "snapshot gaps, deleted ratings).\n\n"
        f"Detector output:\n{json.dumps(compact, indent=2)[:12000]}"
    )
    resp = await client.chat.parse_async(
        model=p.model,
        temperature=0.2,
        response_format=CampaignAssessment,
        messages=[
            {"role": "system", "content": "You are a precise trust & safety research analyst. Output only the structured assessment."},
            {"role": "user", "content": prompt},
        ],
    )
    parsed = resp.choices[0].message.parsed
    return parsed.model_dump() if hasattr(parsed, "model_dump") else parsed


# ── Workflow ──────────────────────────────────────────────────────────────────


@workflows.workflow.define(
    name="cn-astroturf-mapper",
    workflow_display_name="CN Astroturf Mapper",
    workflow_description=(
        "Maps potential X Community Notes astroturfing / coordinated-rating campaigns "
        "against given target handles, using only X's public Community Notes dataset plus "
        "the read-only x.md mirror. Reports coordination clusters of pseudonymous "
        "participant IDs only - never resolved to accounts. Findings are hypotheses for "
        "human review, not proof of a campaign."
    ),
)
class CnAstroturfMapper(workflows.InteractiveWorkflow):
    def __init__(self):
        self.steps = {k: "pending" for k in ("resolve_targets", "load_notes", "load_ratings", "detect", "assess")}

    @workflows.workflow.query(name="get_steps")
    def get_steps(self) -> dict:
        return self.steps

    @workflows.workflow.entrypoint
    async def run(self, payload: AstroturfMapperInput) -> dict:
        p = payload

        self.steps["resolve_targets"] = "running"
        targets = await resolve_target_handles(p)
        self.steps["resolve_targets"] = "done"

        target_tweet_ids = {tid for t in targets for tid in t["tweet_ids"]}

        self.steps["load_notes"] = "running"
        notes = await load_target_notes(target_tweet_ids) if target_tweet_ids else []
        self.steps["load_notes"] = "done"

        target_note_ids = {n["noteId"] for n in notes}

        self.steps["load_ratings"] = "running"
        data = await load_ratings_and_status(target_note_ids, p.detect_flips) if target_note_ids else {
            "ratings_by_note": {}, "status_by_note": {}, "total_ratings_kept": 0,
        }
        self.steps["load_ratings"] = "done"

        self.steps["detect"] = "running"
        findings = await detect_coordination(targets, notes, data, p)
        self.steps["detect"] = "done"

        self.steps["assess"] = "running"
        assessment = await llm_campaign_assessment(findings, p) if findings["per_handle"] else None
        self.steps["assess"] = "done"

        report_markdown = _render_markdown(targets, findings, assessment, p)
        return {
            "workflow": "cn-astroturf-mapper",
            "params": p.model_dump(),
            "targets": targets,
            "total_ratings_kept": data.get("total_ratings_kept", 0),
            "findings": findings,
            "llm_assessment": assessment,
            "report_markdown": report_markdown,
            "disclaimer": (
                "Detection of coordination patterns from X's public Community Notes dataset. "
                "Rater identities remain pseudonymous; no participantId was mapped to any X account. "
                "Coordination signals can also arise from organic partisan brigading — findings are "
                "hypotheses for human review, not proof of a campaign."
            ),
        }


def _render_markdown(targets: list, findings: dict, assessment: Optional[dict], p: AstroturfMapperInput) -> str:
    lines = [
        "# Community Notes astroturfing map",
        "",
        f"- Target handles: {', '.join('@' + t['handle'] for t in targets) or 'n/a'}",
        f"- Detection params: clusters>={p.min_cluster_size} raters, Jaccard>={p.min_co_rating_overlap}, "
        f"bursts>={p.burst_min_ratings} ratings/{p.burst_window_minutes}min one-sided>={p.burst_one_sided_ratio}",
        "",
        "## Per-handle findings",
        "",
    ]
    for h, v in findings["per_handle"].items():
        lines += [
            f"### @{h} — severity: **{v['severity']}** (score {v['coordination_score']}/100)",
            f"- tweets scanned: {v['tweets_scanned']}, notes on their posts: {v['notes_on_their_posts']}",
            f"- co-rating clusters: {len(v['clusters'])} (sizes: {[c['size'] for c in v['clusters']]})",
            f"- rating bursts: {len(v['bursts'])}, status flips: {len(v['status_flips'])}",
            "",
        ]
        for c in v["clusters"][:5]:
            lines.append(
                f"  - cluster `{c['cluster_id']}`: {c['size']} pseudonymous raters, "
                f"mean overlap {c['mean_pairwise_overlap']}, covering {len(c['notes_covered'])} notes"
            )
    if assessment:
        lines += ["", "## Analyst assessment", "", assessment.get("overall_summary", ""), ""]
        for ha in assessment.get("per_handle", []):
            lines += [
                f"### @{ha['handle']} — targeted: {ha['targeted']} ({ha['severity']}, {ha['coordination_score']})",
                ha["summary"],
                "",
                "Evidence:",
            ]
            lines += [f"- {e}" for e in ha["evidence"]]
            lines += ["", "Recommended actions:"]
            lines += [f"- {a}" for a in ha["recommended_actions"]]
            lines.append("")
        lines += ["## Caveats", ""]
        lines += [f"- {c}" for c in assessment.get("methodology_caveats", [])]
    return "\n".join(lines)
