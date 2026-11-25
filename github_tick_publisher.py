"""
github_tick_publisher.py  -  publishes the collector's Bank Nifty & Reliance tick rows to GitHub,
in small per-day / per-5-minute files that the HTML chart can read back (history) and keep in sync with live.

Layout in the repo (labels = FYERS_BANKNIFTY_LABEL & FYERS_RELIANCE_LABEL, default "banknifty", "reliance"):

    data/banknifty/ticks/2026-11-16/0915.csv      <- every row whose time is 09:15:00 - 09:19:59
    data/banknifty/ticks/2026-11-16/0920.csv
    data/reliance/ticks/2026-11-16/0915.csv
    data/reliance/ticks/2026-11-16/0920.csv
    ...

Each file holds rows exactly as the collector writes RawData.csv, WITHOUT a header line:

    20261116 091503 1234560;54321.5;54321.0;54321.5;75

AUTOMATIC CLEANUP (new): only the newest GITHUB_TICK_KEEP_DAYS day-folders are kept (default 10). Older
day-folders are deleted from the branch in ONE commit, once at start-up and then every
GITHUB_TICK_CLEANUP_HOURS (default 6). Today's folder is never deleted. The count is of trading-day
folders that exist (weekends/holidays have none), so "10" means the last 10 trading days.

IMPORTANT ABOUT REPO SIZE: deleting files removes them from the branch, but git keeps every old version in
its history, so the repository's stored size does NOT shrink by deleting alone. To really cap the size use
a dedicated data branch (GITHUB_DATA_BRANCH=data, NOT the branch that holds your code) and set
GITHUB_TICK_SQUASH=true: after a cleanup that deleted something, the branch history is collapsed into a single
commit (force-update). GitHub then garbage-collects the unreachable old data on its own schedule.
NEVER enable SQUASH on a branch that contains your code or whose history you want to keep.

Restart safe: the first time a chunk is touched in a run, the existing file is downloaded from GitHub and
the new rows are appended to it, so a restart in the middle of a chunk never overwrites earlier rows.

Environment variables (the first three are the ones your script already uses):
    GITHUB_TOKEN                  token with "Contents: read and write" on the repo
    GITHUB_REPOSITORY             "owner/repo"
    GITHUB_DATA_BRANCH            branch to write to (falls back to GITHUB_REF_NAME, then "main")
    GITHUB_TICK_LABELS            comma list of instrument labels to publish (default: "banknifty,reliance")
    GITHUB_TICK_PUSH_SECONDS      how often the open chunk is pushed (default 60)
    GITHUB_TICK_CHUNK_MINUTES     chunk length, must divide 60 (default 5)
    GITHUB_TICK_DIR               root folder in the repo (default "data")
    GITHUB_TICK_KEEP_DAYS         newest day-folders to keep, 0 = never delete (default 10)
    GITHUB_TICK_CLEANUP_HOURS     how often the cleanup runs (default 6)
    GITHUB_TICK_SQUASH            "true" = collapse branch history after a cleanup (default false, see warning above)
"""
import asyncio
import base64
import logging
import os
import queue
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")

TOKEN = os.environ.get("GITHUB_TOKEN")
REPO = os.environ.get("GITHUB_REPOSITORY")
BRANCH = os.environ.get("GITHUB_DATA_BRANCH") or os.environ.get("GITHUB_REF_NAME") or "main"
API_BASE = os.environ.get("GITHUB_API_BASE", "https://api.github.com")
ROOT = os.environ.get("GITHUB_TICK_DIR", "data").strip("/")
PUSH_SECONDS = max(15, int(os.environ.get("GITHUB_TICK_PUSH_SECONDS", "60")))
CHUNK_MIN = int(os.environ.get("GITHUB_TICK_CHUNK_MINUTES", "5"))
if CHUNK_MIN < 1 or 60 % CHUNK_MIN != 0:
    CHUNK_MIN = 5
TIMEOUT = int(os.environ.get("GITHUB_REQUEST_TIMEOUT_SECONDS", "60"))
RETRIES = int(os.environ.get("GITHUB_MAX_RETRIES", "3"))
KEEP_DAYS = int(os.environ.get("GITHUB_TICK_KEEP_DAYS", "10"))
CLEANUP_HOURS = max(1.0, float(os.environ.get("GITHUB_TICK_CLEANUP_HOURS", "6")))
SQUASH = os.environ.get("GITHUB_TICK_SQUASH", "false").strip().lower() in ("1", "true", "yes")
DELETE_BATCH = 300                      # files removed per cleanup commit
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_DEFAULT_LABELS = ",".join([
    os.environ.get("FYERS_BANKNIFTY_LABEL", "banknifty"),
    os.environ.get("FYERS_RELIANCE_LABEL", "reliance"),
])
LABELS = {
    x.strip().lower()
    for x in os.environ.get("GITHUB_TICK_LABELS", _DEFAULT_LABELS).split(",")
    if x.strip()
}
ENABLED = bool(TOKEN and REPO)

# rows handed over by the websocket thread; drained by the publisher loop
_q: "queue.SimpleQueue" = queue.SimpleQueue()
# repo path -> {"lines": [...], "existing": set, "sha": str|None, "dirty": bool, "touched": float}
_chunks: dict = {}


def publish_tick(label: str, line: str) -> None:
    """Call this with every RawData.csv row. Cheap and thread-safe: it only queues the row."""
    if not ENABLED or label.lower() not in LABELS:
        return
    try:
        day = line[0:8]                       # yyyyMMdd
        hh = int(line[9:11])
        mm = int(line[11:13])
        if len(day) != 8 or not day.isdigit():
            return
    except (ValueError, IndexError):
        return
    start = (mm // CHUNK_MIN) * CHUNK_MIN
    _q.put((label, f"{day[0:4]}-{day[4:6]}-{day[6:8]}", f"{hh:02d}{start:02d}", line))


def _headers(raw: bool = False) -> dict:
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github.raw+json" if raw else "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _url(path: str) -> str:
    return f"{API_BASE}/repos/{REPO}/contents/{path}"


def _load_chunk(path: str) -> dict:
    """Existing content of one chunk file on GitHub (empty if it does not exist yet)."""
    chunk = {"lines": [], "existing": set(), "sha": None, "dirty": False, "touched": time.time()}
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(_url(path), headers=_headers(), params={"ref": BRANCH}, timeout=TIMEOUT)
            if r.status_code == 404:
                return chunk
            if not r.ok:
                logger.error(f"GitHub read {path}: {r.status_code} {r.text[:200]}")
                return chunk
            data = r.json()
            chunk["sha"] = data.get("sha")
            text = ""
            if data.get("encoding") == "base64" and data.get("content"):
                text = base64.b64decode(data["content"]).decode("utf-8", "replace")
            elif data.get("size"):
                rr = requests.get(_url(path), headers=_headers(raw=True), params={"ref": BRANCH}, timeout=TIMEOUT)
                text = rr.text if rr.ok else ""
            chunk["lines"] = [ln for ln in text.splitlines() if ln.strip()]
            chunk["existing"] = set(chunk["lines"])
            return chunk
        except requests.exceptions.RequestException as e:
            logger.warning(f"GitHub read {path} attempt {attempt}/{RETRIES}: {e}")
            time.sleep(2 * attempt)
    return chunk


def _put_chunk(path: str, chunk: dict) -> bool:
    body = ("\n".join(chunk["lines"]) + "\n").encode("utf-8")
    for attempt in range(1, RETRIES + 1):
        try:
            payload = {
                "message": f"ticks: {path} [skip ci]",
                "content": base64.b64encode(body).decode("ascii"),
                "branch": BRANCH,
            }
            if chunk["sha"]:
                payload["sha"] = chunk["sha"]
            r = requests.put(_url(path), headers=_headers(), json=payload, timeout=TIMEOUT)
            if r.status_code in (409, 422):          # stale sha -> re-read the sha and try again
                g = requests.get(_url(path), headers=_headers(), params={"ref": BRANCH}, timeout=TIMEOUT)
                chunk["sha"] = g.json().get("sha") if g.ok else None
                continue
            if not r.ok:
                logger.error(f"GitHub write {path}: {r.status_code} {r.text[:200]}")
                return False
            chunk["sha"] = (r.json().get("content") or {}).get("sha")
            chunk["dirty"] = False
            return True
        except requests.exceptions.RequestException as e:
            logger.warning(f"GitHub write {path} attempt {attempt}/{RETRIES}: {e}")
            time.sleep(3 * attempt)
    return False


def _drain_and_push() -> None:
    """Moves queued rows into their chunks and uploads every chunk that changed."""
    items = []
    try:
        while True:
            items.append(_q.get_nowait())
    except queue.Empty:
        pass

    now = time.time()
    for label, day, hhmm, line in items:
        path = f"{ROOT}/{label}/ticks/{day}/{hhmm}.csv"
        chunk = _chunks.get(path)
        if chunk is None:
            chunk = _chunks[path] = _load_chunk(path)
        if line in chunk["existing"]:                 # row already on GitHub (restart overlap)
            continue
        chunk["lines"].append(line)
        chunk["dirty"] = True
        chunk["touched"] = now

    pushed = 0
    for path in sorted(p for p, c in _chunks.items() if c["dirty"]):
        if _put_chunk(path, _chunks[path]):
            pushed += 1
        time.sleep(1)                                 # stay well below GitHub's secondary rate limit
    if pushed:
        logger.info(f"[GITHUB TICKS] pushed {pushed} chunk file(s), {len(items)} new row(s)")

    # forget finished chunks so memory stays small
    for path in [p for p, c in _chunks.items() if not c["dirty"] and now - c["touched"] > 3 * CHUNK_MIN * 60]:
        del _chunks[path]


# =========================================================
# --- Automatic cleanup: keep only the newest KEEP_DAYS day-folders ---
# =========================================================
def _git(method: str, path: str, **kw):
    return requests.request(method, f"{API_BASE}/repos/{REPO}/{path}", headers=_headers(), timeout=TIMEOUT, **kw)


def _list_old_day_files() -> list:
    """Repo paths of every file inside day-folders older than the newest KEEP_DAYS (never today's)."""
    today = datetime.now(IST).strftime("%Y-%m-%d")
    paths = []
    for label in sorted(LABELS):
        base = f"{ROOT}/{label}/ticks"
        r = requests.get(_url(base), headers=_headers(), params={"ref": BRANCH}, timeout=TIMEOUT)
        if r.status_code == 404:
            continue
        if not r.ok:
            raise RuntimeError(f"list {base}: {r.status_code} {r.text[:200]}")
        days = sorted(x["name"] for x in r.json() if x.get("type") == "dir" and DAY_RE.match(x["name"]))
        for d in days[:-KEEP_DAYS]:
            if d >= today:                            # safety: never touch today / future folders
                continue
            rr = requests.get(_url(f"{base}/{d}"), headers=_headers(), params={"ref": BRANCH}, timeout=TIMEOUT)
            if not rr.ok:
                raise RuntimeError(f"list {base}/{d}: {rr.status_code} {rr.text[:200]}")
            paths += [f["path"] for f in rr.json() if f.get("type") == "file"]
    return paths


def _delete_paths(paths: list) -> int:
    """Removes the given files from the branch with one commit per DELETE_BATCH files (git trees API)."""
    removed = 0
    for i in range(0, len(paths), DELETE_BATCH):
        part = paths[i:i + DELETE_BATCH]
        for attempt in range(1, RETRIES + 1):
            head = _git("GET", f"git/ref/heads/{BRANCH}").json()["object"]["sha"]
            base_tree = _git("GET", f"git/commits/{head}").json()["tree"]["sha"]
            tr = _git("POST", "git/trees", json={
                "base_tree": base_tree,
                "tree": [{"path": p, "mode": "100644", "type": "blob", "sha": None} for p in part],
            })
            if not tr.ok:
                raise RuntimeError(f"tree: {tr.status_code} {tr.text[:200]}")
            cm = _git("POST", "git/commits", json={
                "message": f"ticks: remove {len(part)} file(s) older than {KEEP_DAYS} days [skip ci]",
                "tree": tr.json()["sha"],
                "parents": [head],
            })
            if not cm.ok:
                raise RuntimeError(f"commit: {cm.status_code} {cm.text[:200]}")
            up = _git("PATCH", f"git/refs/heads/{BRANCH}", json={"sha": cm.json()["sha"]})
            if up.ok:
                removed += len(part)
                break
            time.sleep(2 * attempt)                   # branch moved (another push) -> retry on the new head
        else:
            raise RuntimeError("could not update the branch after retries")
    for p in paths:                                   # drop any cached copy of a deleted file
        _chunks.pop(p, None)
    return removed


def _squash_branch_history() -> None:
    """Collapses the branch into ONE commit with the same files (force-update). Only for a dedicated data branch."""
    head = _git("GET", f"git/ref/heads/{BRANCH}").json()["object"]["sha"]
    tree = _git("GET", f"git/commits/{head}").json()["tree"]["sha"]
    cm = _git("POST", "git/commits", json={
        "message": f"ticks: squashed history, keeping newest {KEEP_DAYS} days [skip ci]",
        "tree": tree,
        "parents": [],
    })
    if not cm.ok:
        raise RuntimeError(f"squash commit: {cm.status_code} {cm.text[:200]}")
    up = _git("PATCH", f"git/refs/heads/{BRANCH}", json={"sha": cm.json()["sha"], "force": True})
    if not up.ok:
        raise RuntimeError(f"squash ref: {up.status_code} {up.text[:200]}")
    logger.info(f"[GITHUB TICKS] history of '{BRANCH}' squashed to one commit (old data is garbage-collected by GitHub).")


def cleanup_old_days() -> None:
    """Deletes day-folders older than the newest KEEP_DAYS. Safe to call at any time."""
    if not ENABLED or KEEP_DAYS <= 0:
        return
    paths = _list_old_day_files()
    if not paths:
        logger.info(f"[GITHUB TICKS] cleanup: nothing older than the newest {KEEP_DAYS} day(s).")
        return
    removed = _delete_paths(paths)
    logger.info(f"[GITHUB TICKS] cleanup: removed {removed} file(s) older than the newest {KEEP_DAYS} day(s).")
    if SQUASH:
        _squash_branch_history()


async def publisher_loop() -> None:
    """Add this to the asyncio.gather(...) in main()."""
    if not ENABLED:
        logger.warning("GitHub tick publisher disabled: set GITHUB_TOKEN and GITHUB_REPOSITORY.")
        return
    logger.info(
        f"GitHub tick publisher on: {REPO}@{BRANCH} -> {ROOT}/<label>/ticks/<date>/<HHMM>.csv "
        f"(labels={sorted(LABELS)}, every {PUSH_SECONDS}s, {CHUNK_MIN}-minute chunks, "
        f"keep newest {KEEP_DAYS} day(s){', history squash ON' if SQUASH else ''})"
    )
    last_cleanup = 0.0
    while True:
        await asyncio.sleep(PUSH_SECONDS)
        try:
            await asyncio.to_thread(_drain_and_push)
        except Exception as e:                        # never let a GitHub hiccup stop data collection
            logger.error(f"GitHub tick publisher error: {e}")
        if KEEP_DAYS > 0 and time.time() - last_cleanup >= CLEANUP_HOURS * 3600:
            last_cleanup = time.time()
            try:                                      # runs after the push, never at the same time as it
                await asyncio.to_thread(cleanup_old_days)
            except Exception as e:
                logger.error(f"GitHub tick cleanup error (will retry in {CLEANUP_HOURS:g} h): {e}")


def flush_now() -> None:
    """Push whatever is queued right now (call on shutdown)."""
    if ENABLED:
        _drain_and_push()
