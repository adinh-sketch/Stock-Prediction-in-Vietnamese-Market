"""
fetch_news_headlines.py
========================

Phase 2 data pull: Vietnamese financial news headlines for the VN30 basket.

Async version. Uses aiohttp + asyncio so the feeds for each stage are fetched
concurrently (per-host connection cap keeps us polite). Wall-time is typically
5-10x faster than the serial version for a full run.

Stages
------
    Stage 0 : Environment check                   (packages, dirs, tickers)
    Stage 2 : CafeF RSS       (cafef.vn)          -> data/raw/news/cafef.parquet
    Stage 3 : VnExpress Kinh Doanh RSS            -> data/raw/news/vnexpress.parquet
    Stage 4 : Vietstock Finance RSS               -> data/raw/news/vietstock.parquet
    Stage 5 : Google News RSS (per-ticker query)  -> data/raw/news/googlenews.parquet
    Stage 6 : Consolidate + de-duplicate          -> data/raw/news/headlines_all.parquet
    Stage 7 : Quality report                      -> data/raw/news/quality_report.txt

Each stage is independent and checkpointed: re-running skips a stage whose
output already exists. Use --force-stage N to regenerate a single stage.

Concurrency model
-----------------
* Global connection limit via TCPConnector(limit=MAX_CONCURRENCY).
* Per-host limit via TCPConnector(limit_per_host=PER_HOST_CONCURRENCY) so
  we never hammer a single domain.
* Per-request retry with exponential backoff wrapped around aiohttp.get.
* A small jittered delay between successive requests to the *same* host.

Schema (all parquet files)
--------------------------
    time         : datetime64[ns]  -- pubDate, UTC
    ticker       : string          -- VN30 code or "" if no match
    source       : string
    language     : string          -- "vi" | "en"
    headline     : string
    snippet      : string
    url          : string
    fetched_at   : datetime64[ns]

Usage
-----
    python fetch_news_headlines.py                 # run all stages
    python fetch_news_headlines.py --dry-run       # env check only
    python fetch_news_headlines.py --force-stage 5 # re-run Google News only

Dependencies
------------
    pip install aiohttp pandas pyarrow --break-system-packages
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import sys
import time
import unicodedata
import urllib.parse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree as ET

import aiohttp

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

# Resolve paths from this script's directory so running from another cwd
# (e.g. `/`) does not attempt to write into a read-only location.
BASE_DIR      = Path(__file__).resolve().parent
DATA_DIR      = BASE_DIR / "data/raw"
NEWS_DIR      = DATA_DIR / "news"
TICKERS_FILE  = DATA_DIR / "included_tickers.json"

USER_AGENT            = ("Mozilla/5.0 (compatible; VN30-Dissertation-Research/1.0; "
                         "+mailto:dinhlenhtienanh.forwork@gmail.com)")
REQUEST_TIMEOUT       = 20      # seconds per HTTP call
MAX_CONCURRENCY       = 16      # total in-flight requests
PER_HOST_CONCURRENCY  = 4       # simultaneous requests to one domain
PER_HOST_MIN_DELAY    = 0.5     # seconds between requests to same host
MAX_RETRIES           = 3
BACKOFF_BASE          = 2.0

# RSS feed lists.
CAFEF_FEEDS = [
    "https://cafef.vn/thi-truong-chung-khoan.rss",
    "https://cafef.vn/doanh-nghiep.rss",
    "https://cafef.vn/tai-chinh-ngan-hang.rss",
    "https://cafef.vn/bat-dong-san.rss",
    "https://cafef.vn/vi-mo-dau-tu.rss",
]

VNEXPRESS_FEEDS = [
    "https://vnexpress.net/rss/kinh-doanh.rss",
    "https://vnexpress.net/rss/kinh-doanh/chung-khoan.rss",
    "https://vnexpress.net/rss/kinh-doanh/doanh-nghiep.rss",
    "https://vnexpress.net/rss/kinh-doanh/ebank.rss",
]

# Some legacy VnExpress category RSS URLs currently return HTML pages.
VNEXPRESS_FALLBACK_FEEDS: dict[str, list[str]] = {
    "https://vnexpress.net/rss/kinh-doanh/chung-khoan.rss": [
        "https://vnexpress.net/rss/kinh-doanh.rss",
    ],
    "https://vnexpress.net/rss/kinh-doanh/doanh-nghiep.rss": [
        "https://vnexpress.net/rss/doanh-nghiep.rss",
    ],
    "https://vnexpress.net/rss/kinh-doanh/ebank.rss": [
        "https://vnexpress.net/rss/kinh-doanh.rss",
    ],
}

VIETSTOCK_FEEDS = [
    "https://vietstock.vn/830/chung-khoan.rss",
    "https://vietstock.vn/737/doanh-nghiep.rss",
    "https://vietstock.vn/145/tai-chinh.rss",
]

GOOGLE_NEWS_TEMPLATE = (
    "https://news.google.com/rss/search"
    "?q={query}&hl=vi&gl=VN&ceid=VN:vi"
)

# --------------------------------------------------------------------------- #
# VN30 ticker -> alias map                                                    #
# --------------------------------------------------------------------------- #

TICKER_ALIASES: dict[str, list[str]] = {
    "ACB": ["ACB", "Ngân hàng Á Châu", "Asia Commercial Bank"],
    "BID": ["BID", "BIDV", "Ngân hàng BIDV", "Ngân hàng Đầu tư và Phát triển"],
    "CTG": ["CTG", "VietinBank", "Ngân hàng Công thương"],
    "FPT": ["FPT", "Tập đoàn FPT"],
    "GAS": ["GAS", "PV GAS", "Khí Việt Nam"],
    "GVR": ["GVR", "Cao su Việt Nam", "Vietnam Rubber"],
    "HDB": ["HDB", "HDBank", "Ngân hàng HDBank"],
    "HPG": ["HPG", "Hòa Phát", "Tập đoàn Hòa Phát"],
    "LPB": ["LPB", "LPBank", "LienVietPostBank", "Ngân hàng Bưu điện Liên Việt"],
    "MBB": ["MBB", "MBBank", "Ngân hàng Quân đội"],
    "MSN": ["MSN", "Masan Group", "Tập đoàn Masan"],
    "MWG": ["MWG", "Thế Giới Di Động", "Đầu tư Thế Giới Di Động"],
    "PLX": ["PLX", "Petrolimex", "Tập đoàn Xăng dầu"],
    "SAB": ["SAB", "Sabeco", "Bia Sài Gòn"],
    "SHB": ["SHB", "Ngân hàng SHB", "Sài Gòn - Hà Nội"],
    "SSI": ["SSI", "Chứng khoán SSI"],
    "STB": ["STB", "Sacombank", "Ngân hàng Sài Gòn Thương Tín"],
    "TCB": ["TCB", "Techcombank", "Ngân hàng Kỹ Thương"],
    "TPB": ["TPB", "TPBank", "Ngân hàng Tiên Phong"],
    "VCB": ["VCB", "Vietcombank", "Ngân hàng Ngoại thương"],
    "VHM": ["VHM", "Vinhomes"],
    "VIB": ["VIB", "Ngân hàng Quốc tế"],
    "VIC": ["VIC", "Vingroup", "Tập đoàn Vingroup"],
    "VJC": ["VJC", "Vietjet", "VietJet Air"],
    "VNM": ["VNM", "Vinamilk", "Sữa Việt Nam"],
    "VPB": ["VPB", "VPBank", "Ngân hàng Việt Nam Thịnh Vượng"],
    "VRE": ["VRE", "Vincom Retail"],
}

# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def clean_text(s: str | None) -> str:
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_pubdate(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:                                # noqa: BLE001
        return None


def load_tickers() -> list[str]:
    """Load ticker symbols from JSON with support for common shapes."""
    if not TICKERS_FILE.exists():
        return sorted(TICKER_ALIASES.keys())

    try:
        data = json.loads(TICKERS_FILE.read_text())
    except Exception as e:                           # noqa: BLE001
        print(f"  WARNING: failed to parse {TICKERS_FILE}: {e}. "
              "Falling back to built-in VN30 alias list.")
        return sorted(TICKER_ALIASES.keys())

    raw: list[str] = []
    if isinstance(data, dict):
        raw = [k for k in data.keys() if isinstance(k, str)]
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                raw.append(item)
                continue
            if isinstance(item, dict):
                for key in ("ticker", "symbol", "code"):
                    value = item.get(key)
                    if isinstance(value, str) and value.strip():
                        raw.append(value)
                        break

    tickers: list[str] = []
    seen: set[str] = set()
    for t in raw:
        tk = t.strip().upper()
        if tk and tk not in seen:
            seen.add(tk)
            tickers.append(tk)

    if not tickers:
        print(f"  WARNING: no valid tickers found in {TICKERS_FILE}. "
              "Falling back to built-in VN30 alias list.")
        return sorted(TICKER_ALIASES.keys())
    return tickers


def match_tickers(text: str) -> list[str]:
    if not text:
        return []
    norm = strip_accents(text)
    hits: set[str] = set()
    for ticker, aliases in TICKER_ALIASES.items():
        for alias in aliases:
            a = strip_accents(alias)
            if len(a) <= 4:
                if re.search(rf"\b{re.escape(a)}\b", norm):
                    hits.add(ticker)
                    break
            else:
                if a in norm:
                    hits.add(ticker)
                    break
    return sorted(hits)


def parse_rss(xml_bytes: bytes) -> list[dict]:
    items: list[dict] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"    XML parse error: {e}")
        return items
    for it in root.iter():
        tag = it.tag.lower().split("}")[-1]
        if tag in ("item", "entry"):
            title = desc = link = pub = ""
            for child in it:
                ctag = child.tag.lower().split("}")[-1]
                if ctag == "title":
                    title = (child.text or "").strip()
                elif ctag in ("description", "summary"):
                    desc = (child.text or "").strip()
                elif ctag == "link":
                    link = (child.text or child.attrib.get("href", "")).strip()
                elif ctag in ("pubdate", "published", "updated"):
                    pub = (child.text or "").strip()
            items.append({"title": title, "description": desc,
                          "link": link, "pubDate": pub})
    return items


# --------------------------------------------------------------------------- #
# Async HTTP layer                                                            #
# --------------------------------------------------------------------------- #

class HostThrottler:
    """Ensure at least PER_HOST_MIN_DELAY between requests to the same host."""
    def __init__(self, min_delay: float):
        self.min_delay = min_delay
        self._locks: dict[str, asyncio.Lock] = {}
        self._last:  dict[str, float] = {}

    def _lock(self, host: str) -> asyncio.Lock:
        if host not in self._locks:
            self._locks[host] = asyncio.Lock()
        return self._locks[host]

    async def wait(self, host: str) -> None:
        async with self._lock(host):
            last = self._last.get(host, 0.0)
            now  = time.monotonic()
            delta = now - last
            if delta < self.min_delay:
                # add a little jitter so parallel workers don't sync up
                await asyncio.sleep(self.min_delay - delta
                                    + random.uniform(0, 0.2))
            self._last[host] = time.monotonic()


async def http_get(session: aiohttp.ClientSession,
                   throttler: HostThrottler,
                   url: str) -> tuple[bytes, str]:
    host = urllib.parse.urlparse(url).netloc
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        await throttler.wait(host)
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(
                    total=REQUEST_TIMEOUT)) as r:
                r.raise_for_status()
                return await r.read(), (r.headers.get("Content-Type") or "")
        except Exception as e:                       # noqa: BLE001
            last_err = e
            wait = BACKOFF_BASE ** attempt + random.uniform(0, 0.5)
            print(f"    attempt {attempt}/{MAX_RETRIES} failed on "
                  f"{url} ({e}); sleeping {wait:.1f}s")
            await asyncio.sleep(wait)
    raise RuntimeError(f"GET {url} failed after {MAX_RETRIES} tries: {last_err}")


def is_xml_payload(payload: bytes, content_type: str) -> bool:
    ct = content_type.lower()
    if any(marker in ct for marker in ("xml", "rss", "atom")):
        return True
    sniff = payload.lstrip()[:256].lower()
    return (sniff.startswith(b"<?xml")
            or sniff.startswith(b"<rss")
            or sniff.startswith(b"<feed"))


async def fetch_feed(session: aiohttp.ClientSession,
                     throttler: HostThrottler,
                     sem: asyncio.Semaphore,
                     url: str,
                     source: str,
                     language: str,
                     force_ticker: str | None = None) -> list[dict]:
    async with sem:
        try:
            xml, content_type = await http_get(session, throttler, url)
        except Exception as e:                       # noqa: BLE001
            print(f"    !! {url} failed: {e}")
            return []

    if not is_xml_payload(xml, content_type):
        recovered = False
        if source == "vnexpress":
            for alt_url in VNEXPRESS_FALLBACK_FEEDS.get(url, []):
                print(f"    {url} returned non-XML ({content_type}); "
                      f"trying fallback {alt_url}")
                async with sem:
                    try:
                        alt_xml, alt_content_type = await http_get(
                            session, throttler, alt_url)
                    except Exception as e:           # noqa: BLE001
                        print(f"    !! fallback {alt_url} failed: {e}")
                        continue
                if is_xml_payload(alt_xml, alt_content_type):
                    xml = alt_xml
                    content_type = alt_content_type
                    recovered = True
                    break

        if not recovered:
            print(f"    !! {url} returned non-XML content-type "
                  f"'{content_type or 'unknown'}'; skipping")
            return []

    raw = parse_rss(xml)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[dict] = []
    for it in raw:
        headline = clean_text(it["title"])
        snippet  = clean_text(it["description"])
        t        = parse_pubdate(it["pubDate"]) or now
        tickers  = match_tickers(headline + " " + snippet)
        if not tickers:
            tickers = [force_ticker] if force_ticker else [""]
        for tk in tickers:
            rows.append({
                "time":       t,
                "ticker":     tk,
                "source":     source,
                "language":   language,
                "headline":   headline,
                "snippet":    snippet,
                "url":        it["link"],
                "fetched_at": now,
            })
    print(f"    {url} -> {len(raw)} items / {len(rows)} rows")
    return rows


# --------------------------------------------------------------------------- #
# Parquet saver                                                               #
# --------------------------------------------------------------------------- #

def save_parquet(rows: list[dict], out: Path) -> None:
    import pandas as pd
    if not rows:
        print(f"    no rows to save for {out.name}")
        return
    df = pd.DataFrame(rows)
    df["time"]       = pd.to_datetime(df["time"])
    df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    for col in ("ticker", "source", "language", "headline", "snippet", "url"):
        df[col] = df[col].astype("string")
    df = df.drop_duplicates(subset=["url", "ticker"]).sort_values("time")
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"    wrote {out} ({len(df):,d} rows)")


# --------------------------------------------------------------------------- #
# Stages                                                                      #
# --------------------------------------------------------------------------- #

def stage_0_env() -> None:
    print("\n[Stage 0] Environment check")
    try:
        import pandas    # noqa: F401
        import pyarrow   # noqa: F401
        import aiohttp   # noqa: F401
    except ImportError as e:
        sys.exit(f"Missing dependency: {e}. "
                 f"Install with `pip install aiohttp pandas pyarrow "
                 f"--break-system-packages`.")
    NEWS_DIR.mkdir(parents=True, exist_ok=True)
    if not TICKERS_FILE.exists():
        print(f"  WARNING: {TICKERS_FILE} not found. "
              f"Falling back to the built-in VN30 alias list.")
    tickers = load_tickers()
    missing = [t for t in tickers if t not in TICKER_ALIASES]
    if missing:
        print(f"  WARNING: no alias defined for {missing}. "
              f"They will still match by code.")
    print(f"  OK. Output dir: {NEWS_DIR.resolve()}")


def _new_session() -> aiohttp.ClientSession:
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENCY,
        limit_per_host=PER_HOST_CONCURRENCY,
        ttl_dns_cache=300,
    )
    headers = {"User-Agent": USER_AGENT,
               "Accept": "application/rss+xml, application/xml, text/xml, */*"}
    return aiohttp.ClientSession(connector=connector, headers=headers)


async def _gather_feeds(feeds: list[tuple[str, str, str, str | None]]
                        ) -> list[dict]:
    """feeds: list of (url, source, language, force_ticker)"""
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    throttler = HostThrottler(PER_HOST_MIN_DELAY)
    async with _new_session() as session:
        tasks = [fetch_feed(session, throttler, sem, u, s, lang, ft)
                 for (u, s, lang, ft) in feeds]
        results = await asyncio.gather(*tasks, return_exceptions=False)
    out: list[dict] = []
    for r in results:
        out.extend(r)
    return out


def stage_cafef(force: bool) -> None:
    out = NEWS_DIR / "cafef.parquet"
    if out.exists() and not force:
        print(f"[Stage 2] skip — {out.name} already exists")
        return
    print("\n[Stage 2] CafeF RSS (async)")
    feeds = [(u, "cafef", "vi", None) for u in CAFEF_FEEDS]
    rows = asyncio.run(_gather_feeds(feeds))
    save_parquet(rows, out)


def stage_vnexpress(force: bool) -> None:
    out = NEWS_DIR / "vnexpress.parquet"
    if out.exists() and not force:
        print(f"[Stage 3] skip — {out.name} already exists")
        return
    print("\n[Stage 3] VnExpress Kinh Doanh RSS (async)")
    feeds = [(u, "vnexpress", "vi", None) for u in VNEXPRESS_FEEDS]
    rows = asyncio.run(_gather_feeds(feeds))
    save_parquet(rows, out)


def stage_vietstock(force: bool) -> None:
    out = NEWS_DIR / "vietstock.parquet"
    if out.exists() and not force:
        print(f"[Stage 4] skip — {out.name} already exists")
        return
    print("\n[Stage 4] Vietstock RSS (async)")
    feeds = [(u, "vietstock", "vi", None) for u in VIETSTOCK_FEEDS]
    rows = asyncio.run(_gather_feeds(feeds))
    save_parquet(rows, out)


def stage_googlenews(force: bool) -> None:
    out = NEWS_DIR / "googlenews.parquet"
    if out.exists() and not force:
        print(f"[Stage 5] skip — {out.name} already exists")
        return
    print("\n[Stage 5] Google News RSS (async, per-ticker)")
    tickers = load_tickers()

    feeds: list[tuple[str, str, str, str | None]] = []
    for tk in tickers:
        aliases = TICKER_ALIASES.get(tk, [tk])
        query_parts = [f'"{a}"' for a in aliases[:3]]
        q = urllib.parse.quote(" OR ".join(query_parts) + " chứng khoán")
        url = GOOGLE_NEWS_TEMPLATE.format(query=q)
        feeds.append((url, "googlenews", "vi", tk))
    rows = asyncio.run(_gather_feeds(feeds))
    save_parquet(rows, out)


def stage_consolidate(force: bool) -> None:
    out = NEWS_DIR / "headlines_all.parquet"
    if out.exists() and not force:
        print(f"[Stage 6] skip — {out.name} already exists")
        return
    print("\n[Stage 6] Consolidate + de-duplicate")
    import pandas as pd
    parts = []
    for name in ("cafef", "vnexpress", "vietstock", "googlenews"):
        p = NEWS_DIR / f"{name}.parquet"
        if p.exists():
            parts.append(pd.read_parquet(p))
    if not parts:
        print("  no source files to consolidate")
        return
    df = pd.concat(parts, ignore_index=True)
    df = (df.sort_values("time")
            .drop_duplicates(subset=["url", "ticker"], keep="first")
            .reset_index(drop=True))
    df.to_parquet(out, index=False)
    print(f"  wrote {out} ({len(df):,d} rows, "
          f"{df['ticker'].nunique()} distinct tickers including unmatched)")


def stage_quality(force: bool) -> None:
    out = NEWS_DIR / "quality_report.txt"
    if out.exists() and not force:
        print(f"[Stage 7] skip — {out.name} already exists")
        return
    print("\n[Stage 7] Quality report")
    import pandas as pd
    src = NEWS_DIR / "headlines_all.parquet"
    if not src.exists():
        print("  headlines_all.parquet missing — run stage 6 first")
        return
    df = pd.read_parquet(src)

    lines: list[str] = []
    lines.append("VN30 news headline scrape — quality report")
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    lines.append(f"generated: {generated_at}")
    lines.append(f"total rows: {len(df):,d}")
    lines.append(f"unique URLs: {df['url'].nunique():,d}")
    lines.append(f"time range: {df['time'].min()} -> {df['time'].max()}")
    lines.append("")
    lines.append("rows per source:")
    for s, n in df.groupby("source").size().items():
        lines.append(f"  {s:<12s} {n:>6d}")
    lines.append("")
    lines.append("top-20 tagged tickers:")
    counts = df[df["ticker"] != ""].groupby("ticker").size().sort_values(
        ascending=False).head(20)
    for t, n in counts.items():
        lines.append(f"  {t:<6s} {n:>5d}")
    untagged = int((df["ticker"] == "").sum())
    lines.append("")
    lines.append(f"untagged rows (no ticker match): {untagged:,d} "
                 f"({100*untagged/max(len(df),1):.1f}%)")
    lines.append("")
    lines.append("NOTE: RSS feeds expose only the most recent 20-200 items.")
    lines.append("For a 2016-2025 historical corpus use GDELT BigQuery or")
    lines.append("Wayback Machine snapshots in a follow-up script.")
    out.write_text("\n".join(lines))
    print(f"  wrote {out}")
    print("\n".join(lines))


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Run stage 0 only (environment check).")
    ap.add_argument("--force-stage", type=int, default=None,
                    help="Force-re-run a single stage (2..7) and exit.")
    args = ap.parse_args()

    stage_0_env()
    if args.dry_run:
        return

    stages = {
        2: stage_cafef,
        3: stage_vnexpress,
        4: stage_vietstock,
        5: stage_googlenews,
        6: stage_consolidate,
        7: stage_quality,
    }

    t0 = time.monotonic()
    if args.force_stage is not None:
        fn = stages.get(args.force_stage)
        if fn is None:
            sys.exit(f"Unknown stage {args.force_stage}")
        fn(force=True)
    else:
        for n in sorted(stages):
            stages[n](force=False)

    print(f"\nAll stages complete in {time.monotonic() - t0:.1f}s.")


if __name__ == "__main__":
    main()
