"""Live slate + odds ingestion from public sources (no API key required).

Built-in source "espn" uses ESPN's public JSON endpoints:

- Scoreboard (the slate):
  https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard
  params: dates=YYYYMMDD, groups=80 (80 = all FBS), limit=N, optional seasontype
- Per-sportsbook odds:
  https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{event_id}/competitions/{competition_id}/odds
  returns one ref per sportsbook; each ref is a full odds item with American prices:
  spread (home handicap, negative = home favored), overUnder, overOdds, underOdds,
  awayTeamOdds {moneyLine, spreadOdds}, homeTeamOdds {moneyLine, spreadOdds}

To add another source: implement fetch_<name>(spec: SourceSpec) -> IngestionResult
and register it in the SOURCES dict at the bottom of this file.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from .models import (
    BookOdds,
    GameOdds,
    IngestionInfo,
    IngestionResult,
    MoneylineOdds,
    SourceSpec,
    SpreadOdds,
    TeamContext,
    TotalOdds,
)

ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
ESPN_CORE_BASE = "https://sports.core.api.espn.com/v2/sports/football/leagues/college-football"
try:
    EASTERN = ZoneInfo("America/New_York")
except Exception:
    EASTERN = timezone(timedelta(hours=-5))  # EST fallback if tzdata is missing
USER_AGENT = "dt-personal-workflows/0.2 (+https://github.com/ThomsenDrake/dt-personal-workflows)"


def _espn_date(date: str | None) -> str:
    """Scoreboard date param (YYYYMMDD). None or "today" = today in America/New_York."""
    if date is None or date.strip().lower() in ("", "today"):
        return datetime.now(EASTERN).strftime("%Y%m%d")
    return date.strip().replace("-", "")


def _as_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _as_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_book_name(name: str) -> str:
    """Collapse provider aliases like "DraftKings - Live Odds" onto the base book name."""
    base = name.split(" - ")[0].strip()
    return base or name.strip()


def _provider_ok(book: str, providers: list[str] | None) -> bool:
    if not providers:
        return True
    wanted = {p.strip().lower() for p in providers}
    return _normalize_book_name(book).strip().lower() in wanted


def _book_from_espn_odds_item(item: dict, fetched_at_utc: str) -> BookOdds | None:
    """Map one ESPN per-sportsbook odds item onto BookOdds. None if no usable market."""
    provider = item.get("provider") or {}
    book = _normalize_book_name(str(provider.get("name") or "unknown"))
    away = item.get("awayTeamOdds") or {}
    home = item.get("homeTeamOdds") or {}

    spread = None
    home_line = _as_float(item.get("spread"))
    if home_line is not None:
        home_price = _as_int(home.get("spreadOdds"))
        away_price = _as_int(away.get("spreadOdds"))
        spread = SpreadOdds(
            home_line=home_line,
            home_price=home_price if home_price is not None else -110,
            away_price=away_price if away_price is not None else -110,
        )

    moneyline = None
    hp = _as_int(home.get("moneyLine"))
    ap = _as_int(away.get("moneyLine"))
    if hp is not None and ap is not None:
        moneyline = MoneylineOdds(home_price=hp, away_price=ap)

    total = None
    line = _as_float(item.get("overUnder"))
    over = _as_int(item.get("overOdds"))
    under = _as_int(item.get("underOdds"))
    if line is not None and over is not None and under is not None:
        total = TotalOdds(line=line, over_price=over, under_price=under)

    if spread is None and moneyline is None and total is None:
        return None
    return BookOdds(book=book, spread=spread, moneyline=moneyline, total=total, last_update_utc=fetched_at_utc)


async def _get_json(client: httpx.AsyncClient, url: str, params: dict | None, sem: asyncio.Semaphore, retries: int = 2) -> object:
    """GET JSON with a semaphore and simple backoff on 429/5xx/network errors."""
    async with sem:
        last_exc: Exception | None = None
        for attempt in range(retries + 1):
            try:
                resp = await client.get(url, params=params)
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                last_exc = exc
                if attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("request failed: " + url)


def _parse_injuries(data: object, limit: int = 12) -> list[str]:
    """Flatten ESPN's team-injuries payload into "Player (POS) - status: detail" strings."""
    if not isinstance(data, dict):
        return []
    out: list[str] = []
    athletes = data.get("athletes")
    if not isinstance(athletes, list):
        return out
    for athlete in athletes:
        if not isinstance(athlete, dict):
            continue
        injuries = athlete.get("injuries")
        if not isinstance(injuries, list) or not injuries:
            continue
        name = str(athlete.get("displayName") or athlete.get("shortName") or "unknown")
        pos = (athlete.get("position") or {}).get("abbreviation") if isinstance(athlete.get("position"), dict) else None
        label = f"{name} ({pos})" if pos else name
        for inj in injuries:
            if not isinstance(inj, dict):
                continue
            status = str(inj.get("status") or "").strip()
            detail = str(inj.get("detail") or inj.get("type") or "").strip()
            if not status and not detail:
                continue
            entry = f"{label} - {status}" if status else label
            if detail:
                entry += f": {detail}"
            out.append(entry)
        if len(out) >= limit:
            break
    return out[:limit]


def _parse_news(data: object, limit: int = 6) -> list[str]:
    """Latest news headlines for a team from ESPN's team-news payload."""
    if not isinstance(data, dict):
        return []
    articles = data.get("articles")
    if not isinstance(articles, list):
        return []
    out: list[str] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        headline = str(article.get("headline") or "").strip()
        if headline:
            out.append(headline)
        if len(out) >= limit:
            break
    return out[:limit]


async def _fetch_team_context(
    client: httpx.AsyncClient,
    team_id: str,
    sem: asyncio.Semaphore,
    warnings: list[str],
) -> TeamContext:
    """Per-team qualitative data (injuries + news) from ESPN's public team endpoints."""
    base = ESPN_SCOREBOARD_URL.rsplit("/scoreboard", 1)[0]
    injuries_url = f"{base}/teams/{team_id}/injuries"
    news_url = f"{base}/teams/{team_id}/news"
    injuries: list[str] = []
    news: list[str] = []
    try:
        injuries = _parse_injuries(await _get_json(client, injuries_url, None, sem))
    except Exception as exc:
        warnings.append(f"injuries fetch failed for team {team_id}: {exc}")
    try:
        news = _parse_news(await _get_json(client, news_url, {"limit": 6}, sem))
    except Exception as exc:
        warnings.append(f"news fetch failed for team {team_id}: {exc}")
    return TeamContext(injuries=injuries, news=news)


async def _fetch_scoreboard_events(client: httpx.AsyncClient, spec: SourceSpec, sem: asyncio.Semaphore) -> list:
    params: dict = {"dates": _espn_date(spec.date), "limit": spec.limit}
    groups = spec.groups or [80]
    params["groups"] = ",".join(str(g) for g in groups)
    if spec.seasontype is not None:
        params["seasontype"] = spec.seasontype
    data = await _get_json(client, ESPN_SCOREBOARD_URL, params, sem)
    if isinstance(data, dict):
        return data.get("events") or []
    return []


async def _fetch_event_books(
    client: httpx.AsyncClient,
    event_id: object,
    competition_id: object,
    spec: SourceSpec,
    sem: asyncio.Semaphore,
    fetched_at_utc: str,
) -> list[BookOdds]:
    """All sportsbooks' lines for one event, from the ESPN core odds API."""
    base = ESPN_CORE_BASE + "/events/" + str(event_id) + "/competitions/" + str(competition_id) + "/odds"
    books: list[BookOdds] = []

    async def load_ref(ref: str) -> BookOdds | None:
        data = await _get_json(client, ref, None, sem)
        if isinstance(data, dict):
            return _book_from_espn_odds_item(data, fetched_at_utc)
        return None

    page = 1
    while page <= 5:
        data = await _get_json(client, base, {"page": page}, sem)
        if not isinstance(data, dict):
            break
        items = data.get("items") or []
        if not items:
            break
        tasks = []
        for item in items:
            if not isinstance(item, dict):
                continue
            provider = item.get("provider") or {}
            name = str(provider.get("name") or "")
            if not _provider_ok(name, spec.providers):
                continue
            ref = item.get("$ref")
            if ref:
                tasks.append(load_ref(str(ref)))
            else:
                b = _book_from_espn_odds_item(item, fetched_at_utc)
                if b is not None:
                    books.append(b)
        if tasks:
            for b in await asyncio.gather(*tasks):
                if b is not None:
                    books.append(b)
        try:
            page_count = int(data.get("pageCount") or 1)
        except (TypeError, ValueError):
            page_count = 1
        if page >= page_count:
            break
        page += 1
    return books


async def fetch_espn(spec: SourceSpec) -> IngestionResult:
    """Ingest the FBS slate + odds from ESPN public APIs for one SourceSpec."""
    fetched_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sem = asyncio.Semaphore(max(1, spec.max_concurrency))
    timeout = httpx.Timeout(max(5.0, spec.timeout_seconds))
    async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": USER_AGENT}, follow_redirects=True) as client:
        events = await _fetch_scoreboard_events(client, spec, sem)
        allowed = {"pre"}
        if spec.include_in_progress:
            allowed.add("in")
        games: list[GameOdds] = []
        scanned = 0
        team_ids_by_game: dict[str, tuple[str, str]] = {}
        for ev in events:
            if not isinstance(ev, dict):
                continue
            status_type = (ev.get("status") or {}).get("type") or {}
            if status_type.get("state") not in allowed:
                continue
            scanned += 1
            comps = ev.get("competitions") or []
            if not comps or not isinstance(comps[0], dict):
                continue
            comp = comps[0]
            home = None
            away = None
            home_id = ""
            away_id = ""
            for c in comp.get("competitors") or []:
                team = (c.get("team") or {}).get("displayName") or "unknown"
                tid = str(c.get("id") or "")
                if c.get("homeAway") == "home":
                    home = team
                    home_id = tid
                else:
                    away = team
                    away_id = tid
            if not home or not away:
                continue
            if home_id and away_id:
                team_ids_by_game["espn-" + str(ev.get("id"))] = (home_id, away_id)
            event_id = ev.get("id")
            comp_id = comp.get("id") or event_id
            books: list[BookOdds] = []
            if spec.per_book:
                try:
                    books = await _fetch_event_books(client, event_id, comp_id, spec, sem, fetched_at_utc)
                except Exception:
                    books = []
            if not books:
                # fallback: consensus odds embedded in the scoreboard payload
                for item in comp.get("odds") or []:
                    if isinstance(item, dict):
                        b = _book_from_espn_odds_item(item, fetched_at_utc)
                        if b is not None and _provider_ok(b.book, spec.providers):
                            books.append(b)
            seen: set[str] = set()
            uniq: list[BookOdds] = []
            for b in books:
                key = b.book.strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                uniq.append(b)
            if not uniq:
                continue
            games.append(
                GameOdds(
                    game_id="espn-" + str(event_id),
                    home_team=home,
                    away_team=away,
                    kickoff_utc=str(ev.get("date") or ""),
                    books=uniq,
                )
            )
        warnings: list[str] = []
        if spec.include_team_context and team_ids_by_game:
            unique_team_ids = sorted({tid for pair in team_ids_by_game.values() for tid in pair})
            contexts = await asyncio.gather(
                *(_fetch_team_context(client, tid, sem, warnings) for tid in unique_team_ids)
            )
            context_by_team = dict(zip(unique_team_ids, contexts))
            for g in games:
                pair = team_ids_by_game.get(g.game_id)
                if not pair:
                    continue
                home_id, away_id = pair
                g.home_context = context_by_team.get(home_id)
                g.away_context = context_by_team.get(away_id)
    books_seen = sorted({b.book for g in games for b in g.books})
    groups_desc = ",".join(str(g) for g in (spec.groups or [80]))
    info = IngestionInfo(
        sources=["espn"],
        fetched_at_utc=fetched_at_utc,
        events_scanned=scanned,
        games_with_odds=len(games),
        books_seen=books_seen,
        detail="ESPN scoreboard dates=" + _espn_date(spec.date) + " groups=" + groups_desc
        + (" per-book lines from ESPN core odds API" if spec.per_book else " consensus lines only")
        + (" + per-team injuries/news" if spec.include_team_context else ""),
        warnings=warnings,
    )
    return IngestionResult(games=games, info=info)


SOURCES = {"espn": fetch_espn}


async def ingest(specs: list[SourceSpec]) -> IngestionResult:
    """Fetch and merge games from all configured sources, deduped by game_id."""
    games: list[GameOdds] = []
    seen: set[str] = set()
    infos: list[IngestionInfo] = []
    for spec in specs:
        fn = SOURCES.get(spec.type.strip().lower())
        if fn is None:
            raise ValueError("Unknown source type: " + repr(spec.type) + " (available: " + ", ".join(sorted(SOURCES)) + ")")
        res = await fn(spec)
        infos.append(res.info)
        for g in res.games:
            if g.game_id in seen:
                continue
            seen.add(g.game_id)
            games.append(g)
    merged = IngestionInfo(
        sources=[s for i in infos for s in i.sources],
        fetched_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        events_scanned=sum(i.events_scanned for i in infos),
        games_with_odds=len(games),
        books_seen=sorted({b for i in infos for b in i.books_seen}),
        detail="; ".join(i.detail for i in infos),
        warnings=[w for i in infos for w in i.warnings],
    )
    return IngestionResult(games=games, info=merged)
