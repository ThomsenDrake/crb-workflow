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

  1. resolve_target_handles  handles -> user id + post IDs (full history when
                             history_scope="full", the default) via x.md
                             (https://mdfromx.com/docs, GET /api/v1/profiles/{handle}/posts;
                             plain GET, no X credentials), or — when input handle_source is
                             "grok_x_search"/"grok_cli"/"auto" — via Grok X Search: the xAI
                             Responses API x_search tool (XAI_API_KEY; billed per post
                             fetched) or the Grok Build CLI in headless mode (its own cached
                             login; headless -p is documented for scripts/CI)
  2. load_target_notes       CN public data: notes attached to those tweets — i.e. the
                             target's community-NOTED tweets. The notes table has no
                             tweet-author column (verified against the live shard header),
                             so the backward join is: handle -> post IDs -> intersect the
                             full notes snapshot; the audit then runs on that noted set
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
  X_MD_BASE_URL   x.md instance to use (default https://mdfromx.com). Point at a
                  self-hosted x.md (e.g. https://xmd.drake-t.workers.dev) to use
                  your own import allowance instead of the shared public one.
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
                    snapshot (default 10; snapshots lag ~48h)
  CN_MAX_SHARDS     max shard index probed per table (default 512; the ratings
                    dump is multiple parts / several GB, so enumerate until a
                    clean 404 rather than stopping at a small cap)
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field

import mistralai.workflows as workflows
import mistralai.workflows.plugins.mistralai as workflows_mistralai

logger = logging.getLogger(__name__)

# ── Input / output models ─────────────────────────────────────────────────────


class AstroturfMapperInput(BaseModel):
    target_handles: list[str] = Field(description="X handles (with or without @) to treat as potential astroturfing targets")
    lookback_days: int = 90
    max_tweets_per_handle: int = 500
    with_replies: bool = False   # include the account's replies when resolving posts (via x.md)
    with_reposts: bool = False   # never true: reposts are other people's posts, not targets' own
    handle_source: Literal["xmd", "grok_x_search", "grok_cli", "auto"] = "grok_cli"  # default grok_cli: Grok Build CLI headless first (one device-login pause per container), x.md fallback per handle on warning; xmd = x.md only; grok_x_search = xAI Responses API x_search only; auto = x.md first with grok fallback on warning
    history_scope: Literal["recent", "full"] = "full"  # "full" (default) = enumerate the handle's whole history (hard cap max_history_posts) so the audit covers ALL community-noted tweets; "recent" = stop at max_tweets_per_handle
    max_history_posts: int = 50000  # hard cap on enumerated posts when history_scope="full"
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


class GrokDeviceLoginInput(BaseModel):
    user_code: str = Field(description="The device code you entered at the verification URL (echoed back to confirm you completed the grok login)")


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
# x.md (default https://mdfromx.com/docs, override with X_MD_BASE_URL for a
# self-hosted instance) is a read-only mirror of public X content. Bulk history:
#   GET {X_MD_BASE_URL}/api/v1/profiles/{handle}/posts
#       ?since=YYYY-MM-DD&max_posts=N&format=json&with_replies=..&with_reposts=false
# Response: {"profile": {...}, "posts": [{"id": "<tweet id>", "text": ...,
# "created_at": ..., "url": "https://x.com/.../status/<id>", ...}],
# "meta": {"count", "truncated", "next_until", ...}}
# Post `id` is the numeric tweet ID — exactly what joins to the CN dataset's
# `tweetId` column. Optional X_MD_API_KEY raises the import rate allowance.


X_MD_BASE_URL = os.environ.get("X_MD_BASE_URL", "https://mdfromx.com")

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


_DEVICE_URL_RE = re.compile(r"https?://[^\s\"']+")
_DEVICE_CODE_PARAM_RE = re.compile(r"user_code=([A-Za-z0-9-]+)")
_DEVICE_DASH_CODE_RE = re.compile(r"\b([A-Z0-9]{4}-[A-Z0-9]{4})\b")


def _parse_device_login_output(text: str) -> dict:
    """Extract the verification URL and user code from `grok login --device-auth` output (RFC 8628)."""
    text = text or ""
    out = {}
    urls = _DEVICE_URL_RE.findall(text)
    url = (
        next((u for u in urls if "user_code=" in u), None)
        or next((u for u in urls if "device" in u or "oauth" in u), None)
        or (urls[0] if urls else None)
    )
    if url:
        out["verification_url"] = url
    m = _DEVICE_CODE_PARAM_RE.search(text) or _DEVICE_DASH_CODE_RE.search(text)
    if m:
        out["user_code"] = m.group(1)
    return out


_USER_ID_RE = re.compile(r"(?:USER_ID|userId|user_id)\"?\s*[:=]\s*\"?(\d{5,})\"?", re.IGNORECASE)


def _parse_user_id(text: str) -> Optional[str]:
    """Numeric X user id from a resolver's `USER_ID: <id>` line or `"userId": "<id>"` JSON (grok sources echo it on request)."""
    m = _USER_ID_RE.search(text or "")
    return m.group(1) if m else None


def _post_cap(p: AstroturfMapperInput) -> int:
    """Effective enumeration cap: full history walk when history_scope='full', else the recent-posts cap."""
    return p.max_history_posts if p.history_scope == "full" else p.max_tweets_per_handle


async def _resolve_handle_xmd(client, handle: str, since: str, p: AstroturfMapperInput) -> dict:
    """x.md bulk-history route for one handle (see module docs). Degrades to a per-handle warning on 401/403/404/other 4xx/5xx."""
    seen = set()
    tweet_ids = []
    until = None
    body = {}
    profile = {}
    warning = None
    cap = _post_cap(p)
    while len(tweet_ids) < cap:
        params = {
            "since": since,
            "max_posts": min(5000, cap - len(tweet_ids) + 1),
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
        if r.status_code == 401:
            warning = (
                f"x.md returned HTTP 401 for @{handle}: the x.md API key is missing or invalid; "
                "bind the x_md_api_key secret to this deployment (env var X_MD_API_KEY or x_md_api_key)"
            )
            break
        if r.status_code in (403, 404):
            warning = "profile not found or not publicly readable via x.md"
            break
        if r.status_code >= 400:
            # 5xx bulk-import hiccups and any other 4xx degrade per-handle; surface the error body
            detail = " ".join(r.text[:200].split())
            warning = f"x.md returned HTTP {r.status_code} for @{handle}; no posts resolved for this handle"
            if detail:
                warning += f" — {detail}"
            break
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
        if body.get("profile"):
            profile = body["profile"]  # first page carries the profile record
        meta = body.get("meta", {})
        if meta.get("truncated") and meta.get("next_until") and len(tweet_ids) < cap:
            until = meta["next_until"]  # continue the walk; dedupe boundary via `seen`
        else:
            break
    entry = {
        "handle": handle,
        "user_id": profile.get("id"),
        "tweet_ids": tweet_ids[:cap],
        "tweets_scanned": len(tweet_ids[:cap]),
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
    cap = _post_cap(p)
    query = f"from:{handle} -is:retweet" + ("" if p.with_replies else " -is:reply")
    prompt = (
        f"Use X Search only (no web search). First, use X Search user search to find @{handle} and put "
        f"'USER_ID: <numeric profile id>' on the FIRST line of your reply. Then list up to {cap} original "
        f"posts by @{handle} posted from {since} through {until} (query: {query}) as full x.com post URLs "
        "(https://x.com/<handle>/status/<id>), one per line, most recent first. This is read-only: do not "
        "post, reply, like, repost, or follow anything. If no posts are found, reply exactly: NONE"
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
    if r.status_code >= 400:
        entry["warning"] = f"xAI returned HTTP {r.status_code} for grok x_search on @{handle}"
        return entry
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
    entry["user_id"] = _parse_user_id("\n".join(texts))
    entry["tweet_ids"] = ids[:cap]
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
    cap = _post_cap(p)
    query = f"from:{handle} -is:retweet" + ("" if p.with_replies else " -is:reply")
    prompt = (
        f"Use X Search (x_search) only, skip web search. First run a user search for @{handle} and put "
        f"'USER_ID: <numeric profile id>' on the FIRST line of your reply. Then list up to {cap} original "
        f"posts by @{handle} posted from {since} through {until} (query: {query}) as full x.com post URLs "
        "(https://x.com/<handle>/status/<id>), one per line, most recent first. This task is read-only: "
        "do not post, reply, like, repost, or follow anything. If no posts are found, reply exactly: NONE"
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
    out_text = out_b.decode(errors="replace")
    ids = _extract_status_ids(out_text, handle)
    entry["user_id"] = _parse_user_id(out_text)
    entry["tweet_ids"] = ids[:cap]
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


def _grok_home() -> str:
    return os.environ.get("GROK_HOME") or os.path.expanduser("~/.grok")


def _pid_alive(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[-1].split()[0] != "Z"
    except OSError:
        return False


@workflows.activity(start_to_close_timeout=timedelta(minutes=2), retry_policy_max_attempts=1)
async def grok_device_login_start() -> dict:
    """Start the grok CLI device-code login (RFC 8628) when no credential is present.

    The login process keeps polling in the background and writes the session to
    GROK_HOME/~/.grok/auth.json. Returns status + the user-facing verification URL and
    code — never token material (log contents are not returned).
    """
    import shutil
    import subprocess

    bin_path = os.environ.get("GROK_BIN", "grok")
    if shutil.which(bin_path) is None and not os.path.exists(bin_path):
        return {"status": "no_binary", "detail": f"grok CLI not found ({bin_path})"}
    if os.environ.get("XAI_API_KEY"):
        return {"status": "authenticated", "detail": "XAI_API_KEY set"}
    if os.path.exists(os.path.join(_grok_home(), "auth.json")):
        return {"status": "authenticated", "detail": "cached login present"}
    run_dir = tempfile.mkdtemp(prefix="grok-login-")
    log_path = os.path.join(run_dir, "login.log")
    log_f = open(log_path, "wb")
    proc = subprocess.Popen([bin_path, "login", "--device-auth"], stdout=log_f, stderr=subprocess.STDOUT, env=os.environ.copy(), cwd=run_dir)
    deadline = datetime.now(timezone.utc) + timedelta(seconds=20)
    parsed = {}
    while datetime.now(timezone.utc) < deadline:
        try:
            with open(log_path, "r", errors="replace") as f:
                parsed = _parse_device_login_output(f.read())
        except OSError:
            pass
        if parsed.get("verification_url") and parsed.get("user_code"):
            return {"status": "pending", "verification_url": parsed["verification_url"], "user_code": parsed["user_code"], "pid": proc.pid}
        if proc.poll() is not None:
            break
        await asyncio.sleep(1)
    return {"status": "failed", "detail": "grok login exited before printing a verification URL", "pid": proc.pid}


@workflows.activity(start_to_close_timeout=timedelta(minutes=12), retry_policy_max_attempts=1)
async def grok_device_login_wait(pid: int, timeout_seconds: int = 600) -> dict:
    """Wait for the background device-code login to complete (auth.json present and `grok models` succeeds)."""
    import shutil
    import subprocess

    bin_path = os.environ.get("GROK_BIN", "grok")
    auth_json = os.path.join(_grok_home(), "auth.json")
    deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)
    while datetime.now(timezone.utc) < deadline:
        if os.path.exists(auth_json) and shutil.which(bin_path) is not None or os.path.exists(bin_path):
            try:
                probe = subprocess.run([bin_path, "models"], capture_output=True, timeout=60, env=os.environ.copy())
                if probe.returncode == 0:
                    return {"status": "authenticated"}
            except (OSError, subprocess.TimeoutExpired):
                pass
        if not _pid_alive(pid) and not os.path.exists(auth_json):
            return {"status": "failed", "detail": "grok login process exited before completing"}
        await asyncio.sleep(5)
    return {"status": "timeout"}


@workflows.activity(start_to_close_timeout=timedelta(minutes=15), retry_policy_max_attempts=3)
async def resolve_target_handles(p: AstroturfMapperInput) -> list[dict]:
    import httpx

    since = (datetime.now(timezone.utc) - timedelta(days=p.lookback_days)).strftime("%Y-%m-%d")
    until = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")  # x_search to_date is exclusive
    out = []
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("X_MD_API_KEY") or os.environ.get("x_md_api_key")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    async with httpx.AsyncClient(base_url=X_MD_BASE_URL, headers=headers, timeout=300.0) as client:
        for raw in p.target_handles:
            handle = raw.lstrip("@")
            if p.handle_source in ("grok_x_search", "grok_cli"):
                resolver = _resolve_handle_grok_x_search if p.handle_source == "grok_x_search" else _resolve_handle_grok_cli
                try:
                    entry = await resolver(handle, since, until, p)
                except RuntimeError:
                    raise  # 429 stays actionable and fails the run
                except httpx.HTTPError as e:
                    entry = {"handle": handle, "user_id": None, "tweet_ids": [], "tweets_scanned": 0,
                             "source": p.handle_source, "warning": f"resolver request failed for @{handle}: {e}"}
                if p.handle_source == "grok_cli" and entry.get("warning"):
                    # Grok CLI unavailable/failed for this handle (no binary, not signed in, no posts) -> x.md fallback
                    try:
                        x = await _resolve_handle_xmd(client, handle, since, p)
                    except RuntimeError:
                        raise
                    except httpx.HTTPError as e:
                        x = {"tweet_ids": [], "warning": f"x.md request failed: {e}"}
                    if x.get("tweet_ids"):
                        entry["tweet_ids"] = x["tweet_ids"]
                        entry["tweets_scanned"] = x["tweets_scanned"]
                        if x.get("user_id"):
                            entry["user_id"] = x["user_id"]
                        entry["source"] = "grok_cli-warning+xmd"
                        entry["warning"] = f"grok_cli: {entry['warning']}; recovered via x.md"
                    else:
                        entry["warning"] = f"grok_cli: {entry['warning']}; x.md: {x.get('warning', 'no posts')}"
                out.append(entry)
                continue
            try:
                entry = await _resolve_handle_xmd(client, handle, since, p)
            except RuntimeError:
                raise  # 429 stays actionable and fails the run
            except httpx.HTTPError as e:
                entry = {"handle": handle, "user_id": None, "tweet_ids": [], "tweets_scanned": 0,
                         "source": "xmd", "warning": f"x.md request failed for @{handle}: {e}"}
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
CN_MAX_SHARDS = int(os.environ.get("CN_MAX_SHARDS", "512"))
CN_LOOKBACK_DAYS = int(os.environ.get("CN_LOOKBACK_DAYS", "10"))  # snapshots are daily best-effort with ~48h lag; walk back if today's is missing
CN_PROBE_MAX_ATTEMPTS = 6  # attempts per shard URL before giving up on it (429/5xx/timeout/network retried with backoff)
CN_PROBE_BACKOFF_BASE_S = 0.5
# table -> candidate (dir, prefix) pairs; X has used both camelCase and kebab-case dir names
CN_TABLES = {
    "notes": [("notes", "notes")],
    "ratings": [("noteRatings", "ratings"), ("ratings", "ratings")],
    "status": [("noteStatusHistory", "noteStatusHistory"), ("note-status-history", "note-status-history")],
}


async def _backoff_sleep(attempt: int) -> None:
    """Exponential backoff with jitter between probe retries (attempt is 0-based)."""
    delay = min(30.0, CN_PROBE_BACKOFF_BASE_S * (2 ** attempt)) + random.uniform(0.0, 0.5)
    await asyncio.sleep(delay)


def _fill_diag(diag: Optional[dict], info: dict) -> None:
    if diag is not None:
        diag.clear()
        diag.update(info)


async def _probe_url_ranged_get(client: httpx.AsyncClient, url: str) -> str:
    """HEAD returned 405 (method not allowed): fall back to a ranged GET to test existence."""
    import httpx

    for attempt in range(CN_PROBE_MAX_ATTEMPTS):
        try:
            r = await client.get(url, headers={"Range": "bytes=0-0"}, follow_redirects=True)
        except httpx.HTTPError:
            r = None
        if r is not None:
            if r.status_code in (200, 206):
                return "ok"
            if r.status_code == 404:
                return "not_found"
            if r.status_code == 416:
                return "ok"  # object exists but is empty
        await _backoff_sleep(attempt)
    return "error"


async def _probe_url(client: httpx.AsyncClient, url: str) -> str:
    """Classify one shard URL.

    Returns "ok" (exists), "not_found" (clean 404 — normal end of contiguous shard
    enumeration), or "error" (429/5xx/timeout/network, retried with exponential
    backoff + jitter up to CN_PROBE_MAX_ATTEMPTS before giving up on this URL).
    HEAD 405 falls back to a ranged GET.
    """
    import httpx

    for attempt in range(CN_PROBE_MAX_ATTEMPTS):
        try:
            r = await client.head(url, follow_redirects=True)
        except httpx.HTTPError:
            r = None
        if r is not None:
            if r.status_code == 200:
                return "ok"
            if r.status_code == 404:
                return "not_found"
            if r.status_code == 405:
                return await _probe_url_ranged_get(client, url)
        await _backoff_sleep(attempt)
    return "error"


async def _shard_urls(client: httpx.AsyncClient, candidates: list, diag: Optional[dict] = None) -> list[str]:
    """Resolve the current snapshot's shard URLs for a table.

    Layout (as of 2026): {CN_DATA_BASE}/YYYY/MM/DD/<table-dir>/<prefix>-NNNNN.zip,
    one snapshot per day. Walk back up to CN_LOOKBACK_DAYS from today until shard
    00000 exists, then enumerate contiguous shards until a clean 404 (or the
    CN_MAX_SHARDS cap). Falls back to the legacy undated layout.

    Probe outcomes are classified (see _probe_url): 404 ends enumeration, transient
    errors are retried with backoff, HEAD 405 falls back to a ranged GET. [] is
    returned ONLY after every candidate dir/layout and every date dir has been
    exhausted with retries — never silently on a single non-200.

    If `diag` is given it is filled with the resolution details: snapshot_date
    ("YYYY/MM/DD", "legacy", or None), table_dir, prefix, shard_count, shard_urls,
    complete (False when enumeration stopped on a transient error or the cap
    rather than a clean 404).
    """
    today = datetime.now(timezone.utc).date()
    date_dirs = [(today - timedelta(days=i)).strftime("%Y/%m/%d") for i in range(CN_LOOKBACK_DAYS)]
    date_dirs.append("")  # legacy undated layout
    for table_dir, prefix in candidates:
        for dd in date_dirs:
            base = f"{CN_DATA_BASE}/{dd}" if dd else CN_DATA_BASE
            first = f"{base}/{table_dir}/{prefix}-00000.zip"
            if await _probe_url(client, first) != "ok":
                continue  # no snapshot in this dir (404) or unreachable after retries — try the next
            urls = [first]
            complete = False
            for i in range(1, CN_MAX_SHARDS):
                url = f"{base}/{table_dir}/{prefix}-{i:05d}.zip"
                outcome = await _probe_url(client, url)
                if outcome == "ok":
                    urls.append(url)
                else:
                    # "not_found" = clean end of contiguous shards; "error" = retries
                    # exhausted mid-list, keep the partial and flag it incomplete
                    complete = outcome == "not_found"
                    break
            _fill_diag(diag, {
                "snapshot_date": dd or "legacy",
                "table_dir": table_dir,
                "prefix": prefix,
                "shard_count": len(urls),
                "shard_urls": list(urls),
                "complete": complete,
            })
            return urls
        # legacy unsharded single file
        single = f"{CN_DATA_BASE}/{table_dir}/{prefix}.zip"
        if await _probe_url(client, single) == "ok":
            _fill_diag(diag, {
                "snapshot_date": "legacy",
                "table_dir": table_dir,
                "prefix": prefix,
                "shard_count": 1,
                "shard_urls": [single],
                "complete": True,
            })
            return [single]
    _fill_diag(diag, {
        "snapshot_date": None,
        "table_dir": candidates[0][0] if candidates else None,
        "prefix": candidates[0][1] if candidates else None,
        "shard_count": 0,
        "shard_urls": [],
        "complete": False,
    })
    return []


async def _download_shard(client: httpx.AsyncClient, url: str, tmp_path: str) -> None:
    """Download one shard zip to tmp_path, retrying transient failures with backoff.

    A dropped connection mid-shard (RemoteProtocolError) or a 429/5xx response must not
    kill the whole load — restart the download (rows are only parsed after the full
    zip is on disk, so a restart never yields partial data).
    """
    import httpx

    last_exc: Optional[Exception] = None
    for attempt in range(CN_PROBE_MAX_ATTEMPTS):
        try:
            async with client.stream("GET", url, follow_redirects=True, timeout=600.0) as resp:
                resp.raise_for_status()
                with open(tmp_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=1 << 20):
                        f.write(chunk)
            return
        except httpx.HTTPError as e:
            last_exc = e
            logger.warning("shard download attempt %s/%s failed for %s: %s", attempt + 1, CN_PROBE_MAX_ATTEMPTS, url, e)
            if attempt < CN_PROBE_MAX_ATTEMPTS - 1:
                await _backoff_sleep(attempt)
    raise last_exc  # type: ignore[misc]


async def _aiter_tsv_rows(client: httpx.AsyncClient, url: str):
    """Async generator streaming a zipped TSV shard row-by-row (dict rows keyed by header).

    The ratings table is ~184M rows across many shards, so shards are streamed to a
    temp file and parsed incrementally — never loaded into memory wholesale.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp_path = tmp.name
    tmp.close()
    try:
        await _download_shard(client, url, tmp_path)
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


def _log_source(table: str, source: dict) -> None:
    logger.info(
        "cn data source %s: snapshot_date=%s shards=%s complete=%s rows_scanned=%s rows_kept=%s",
        table,
        source.get("snapshot_date"),
        source.get("shard_count"),
        source.get("complete"),
        source.get("rows_scanned"),
        source.get("rows_kept"),
    )


@workflows.activity(start_to_close_timeout=timedelta(hours=2), retry_policy_max_attempts=1)
async def load_target_notes(target_tweet_ids: set) -> dict:
    """Notes attached to any target tweet, plus the resolved notes-table source info."""
    import httpx

    diag: dict = {}
    async with httpx.AsyncClient(timeout=120.0) as client:
        urls = await _shard_urls(client, CN_TABLES["notes"], diag)
        logger.info("cn source resolved: table=notes snapshot_date=%s shards=%s complete=%s", diag.get("snapshot_date"), diag.get("shard_count"), diag.get("complete"))
        notes = []
        rows_scanned = 0
        for url in urls:
            async for row in _aiter_tsv_rows(client, url):
                rows_scanned += 1
                if row.get("tweetId") in target_tweet_ids:
                    notes.append({
                        "noteId": row.get("noteId"),
                        "tweetId": row.get("tweetId"),
                        "author_participant_id": row.get("participantId"),  # pseudonymous only
                        "createdAtMillis": int(row.get("createdAtMillis") or 0),
                        "classification": row.get("classification"),
                        "summary_excerpt": (row.get("summary") or "")[:300],
                    })
    source = {**diag, "rows_scanned": rows_scanned, "rows_kept": len(notes)}
    _log_source("notes", source)
    return {"notes": notes, "source": source}


@workflows.activity(start_to_close_timeout=timedelta(hours=3), retry_policy_max_attempts=1)
async def load_ratings_and_status(target_note_ids: set, detect_flips: bool) -> dict:
    """Ratings + note status history restricted to target notes (streamed), plus source info per table."""
    import httpx

    ratings_by_note = defaultdict(list)
    status_by_note = defaultdict(list)
    ratings_diag: dict = {}
    status_diag: dict = {}
    ratings_rows_scanned = 0
    status_rows_scanned = 0
    async with httpx.AsyncClient(timeout=120.0) as client:
        # ratings
        ratings_urls = await _shard_urls(client, CN_TABLES["ratings"], ratings_diag)
        logger.info("cn source resolved: table=ratings snapshot_date=%s shards=%s complete=%s", ratings_diag.get("snapshot_date"), ratings_diag.get("shard_count"), ratings_diag.get("complete"))
        for url in ratings_urls:
            async for row in _aiter_tsv_rows(client, url):
                ratings_rows_scanned += 1
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
            status_urls = await _shard_urls(client, CN_TABLES["status"], status_diag)
            logger.info("cn source resolved: table=status snapshot_date=%s shards=%s complete=%s", status_diag.get("snapshot_date"), status_diag.get("shard_count"), status_diag.get("complete"))
            for url in status_urls:
                async for row in _aiter_tsv_rows(client, url):
                    status_rows_scanned += 1
                    nid = row.get("noteId")
                    if nid in target_note_ids:
                        status = row.get("currentStatus") or row.get("mostRecentNonNMRStatus") or ""
                        ts = row.get("timestampMillisOfCurrentStatus") or row.get("createdAtMillis") or 0
                        status_by_note[nid].append({
                            "noteId": nid,
                            "createdAtMillis": int(ts or 0),
                            "status": (status or "").upper(),
                        })
    ratings_source = {
        **ratings_diag,
        "rows_scanned": ratings_rows_scanned,
        "rows_kept": sum(len(v) for v in ratings_by_note.values()),
    }
    status_source = (
        {
            **status_diag,
            "rows_scanned": status_rows_scanned,
            "rows_kept": sum(len(v) for v in status_by_note.values()),
        }
        if detect_flips
        else None
    )
    _log_source("ratings", ratings_source)
    if status_source is not None:
        _log_source("status", status_source)
    return {
        "ratings_by_note": {k: v for k, v in ratings_by_note.items()},
        "status_by_note": {k: v for k, v in status_by_note.items()},
        "total_ratings_kept": sum(len(v) for v in ratings_by_note.values()),
        "ratings_source": ratings_source,
        "status_source": status_source,
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
            "noted_tweet_ids": sorted({n["tweetId"] for n in h_notes}),
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
                "noted_tweets": len(v.get("noted_tweet_ids") or []),
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
        super().__init__()  # InteractiveWorkflow: _pending_inputs / _pending_wait_count (needed by wait_for_input)
        self.steps = {k: "pending" for k in ("resolve_targets", "load_notes", "load_ratings", "detect", "assess")}
        self.grok_auth: Optional[dict] = None

    @workflows.workflow.query(name="get_steps")
    def get_steps(self) -> dict:
        return self.steps

    @workflows.workflow.query(name="get_grok_auth")
    def get_grok_auth(self) -> Optional[dict]:
        return self.grok_auth

    @workflows.workflow.entrypoint
    async def run(self, payload: AstroturfMapperInput) -> dict:
        p = payload

        grok_auth = None
        if p.handle_source in ("grok_cli", "auto"):
            start = await grok_device_login_start()
            if start.get("status") == "pending":
                label = f"grok device login: open {start['verification_url']} and enter code {start['user_code']}"
                try:
                    submission = await self.wait_for_input(GrokDeviceLoginInput, label=label, timeout=timedelta(minutes=10))
                    wait = await grok_device_login_wait(start["pid"])
                    grok_auth = {**start, "status": wait["status"], "submitted_user_code": submission.user_code}
                except asyncio.TimeoutError:
                    grok_auth = {**start, "status": "input_timeout"}
            else:
                grok_auth = start
            self.grok_auth = grok_auth

        self.steps["resolve_targets"] = "running"
        targets = await resolve_target_handles(p)
        self.steps["resolve_targets"] = "done"

        target_tweet_ids = {tid for t in targets for tid in t["tweet_ids"]}

        self.steps["load_notes"] = "running"
        empty_source = {"snapshot_date": None, "table_dir": None, "prefix": None, "shard_count": 0, "shard_urls": [], "complete": False, "rows_scanned": 0, "rows_kept": 0}
        notes_res = await load_target_notes(target_tweet_ids) if target_tweet_ids else {"notes": [], "source": dict(empty_source)}
        notes = notes_res["notes"]
        notes_source = notes_res["source"]
        self.steps["load_notes"] = "done"

        target_note_ids = {n["noteId"] for n in notes}

        self.steps["load_ratings"] = "running"
        data = await load_ratings_and_status(target_note_ids, p.detect_flips) if target_note_ids else {
            "ratings_by_note": {}, "status_by_note": {}, "total_ratings_kept": 0,
            "ratings_source": dict(empty_source), "status_source": None,
        }
        self.steps["load_ratings"] = "done"

        ratings_source = data.get("ratings_source") or dict(empty_source)
        status_source = data.get("status_source")
        notes_shards = notes_source.get("shard_count", 0)
        # Loud failure instead of a silent score-0 result: if we have target notes
        # (so the notes table resolved) but the ratings/status tables resolved to
        # nothing, the run must NOT look like a clean "no coordination" pass.
        ratings_source_unavailable = bool(
            target_note_ids
            and notes_shards > 0
            and (ratings_source.get("shard_count", 0) == 0 or ratings_source.get("rows_scanned", 0) == 0)
        )
        status_source_unavailable = bool(
            p.detect_flips
            and target_note_ids
            and notes_shards > 0
            and status_source is not None
            and (status_source.get("shard_count", 0) == 0 or status_source.get("rows_scanned", 0) == 0)
        )

        self.steps["detect"] = "running"
        findings = await detect_coordination(targets, notes, data, p)
        self.steps["detect"] = "done"

        if ratings_source_unavailable:
            for v in findings["per_handle"].values():
                v["coordination_score"] = None  # null, not 0: the ratings source was unavailable
                v["severity"] = "unknown"

        self.steps["assess"] = "running"
        assessment = await llm_campaign_assessment(findings, p) if findings["per_handle"] else None
        self.steps["assess"] = "done"

        data_sources = {
            "notes": notes_source,
            "ratings": ratings_source,
            "status": status_source if status_source is not None else {**empty_source, "disabled": not p.detect_flips},
        }
        logger.info("cn-astroturf-mapper data_sources: %s", json.dumps(data_sources, default=str)[:20000])
        if ratings_source_unavailable:
            logger.warning("ratings source unavailable: notes table resolved %s shards but ratings resolved %s shards / %s rows scanned", notes_shards, ratings_source.get("shard_count", 0), ratings_source.get("rows_scanned", 0))
        if status_source_unavailable:
            logger.warning("noteStatusHistory source unavailable: notes table resolved %s shards but status resolved %s shards", notes_shards, (status_source or {}).get("shard_count", 0))

        report_markdown = _render_markdown(targets, findings, assessment, p, data_sources=data_sources, ratings_source_unavailable=ratings_source_unavailable, status_source_unavailable=status_source_unavailable)
        return {
            "workflow": "cn-astroturf-mapper",
            "params": p.model_dump(),
            "grok_auth": grok_auth,
            "targets": targets,
            "total_ratings_kept": data.get("total_ratings_kept", 0),
            "data_sources": data_sources,
            "ratings_source_unavailable": ratings_source_unavailable,
            "status_source_unavailable": status_source_unavailable,
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


def _render_markdown(
    targets: list,
    findings: dict,
    assessment: Optional[dict],
    p: AstroturfMapperInput,
    data_sources: Optional[dict] = None,
    ratings_source_unavailable: bool = False,
    status_source_unavailable: bool = False,
) -> str:
    lines = [
        "# Community Notes astroturfing map",
        "",
        f"- Target handles: {', '.join('@' + t['handle'] for t in targets) or 'n/a'}",
        f"- Detection params: clusters>={p.min_cluster_size} raters, Jaccard>={p.min_co_rating_overlap}, "
        f"bursts>={p.burst_min_ratings} ratings/{p.burst_window_minutes}min one-sided>={p.burst_one_sided_ratio}",
        "",
    ]
    if ratings_source_unavailable:
        lines += [
            "> **WARNING: the ratings data source was unavailable for this run** (the notes table resolved, "
            "but the ratings table resolved 0 shards/rows). Coordination score is null, not 0 — this is NOT a "
            "clean 'no coordination' result. Re-run when ton.twimg.com is reachable.",
            "",
        ]
    if status_source_unavailable:
        lines += [
            "> **WARNING: the noteStatusHistory data source was unavailable for this run**; status-flip detection "
            "saw no data.",
            "",
        ]
    lines += ["## Per-handle findings", ""]

    for h, v in findings["per_handle"].items():
        lines += [
            f"### @{h} — severity: **{v['severity']}** (score {v['coordination_score']}/100)",
            f"- tweets scanned: {v['tweets_scanned']}, notes on their posts: {v['notes_on_their_posts']}",
            f"- noted tweets audited (posts with Community Notes): {len(v.get('noted_tweet_ids') or [])} (of {v['tweets_scanned']} scanned; history_scope={p.history_scope})",
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
    if data_sources:
        lines += ["", "## Data sources", ""]
        for table, src in data_sources.items():
            lines.append(
                f"- {table}: snapshot={src.get('snapshot_date') or 'none'}, shards={src.get('shard_count', 0)}"
                f" (complete={src.get('complete')}), rows_scanned={src.get('rows_scanned', 0)}, rows_kept={src.get('rows_kept', 0)}"
            )
    return "\n".join(lines)
