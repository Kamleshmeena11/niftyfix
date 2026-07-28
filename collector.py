import os
import sys
import time
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

# Upstox SDK & Streaming Tools
import upstox_client
from upstox_client.feeder import MarketDataStreamerV3

# Google Drive Modules
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials

# --- Logging Setup ---
# Force unbuffered/line-buffered stdout. Python defaults to full block
# buffering when stdout isn't a real terminal (e.g. piped into GitHub
# Actions' log capture), which can delay or batch log lines for long
# stretches -- looking exactly like a process freeze/restart when it was
# actually just delayed console output, while file writes (CSV) were never
# affected since those flush on their own each call.
sys.stdout.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Timezone ---
IST = ZoneInfo("Asia/Kolkata")

# --- Configuration & Credentials ---
UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN")

# Instruments being tracked. "label" drives every filename below --
# nifty_2026-07-20.csv / nifty_ALL.csv, bank_nifty_2026-07-20.csv / bank_nifty_ALL.csv.
INSTRUMENTS = {
    "NSE_INDEX|Nifty 50": {"label": "nifty"},
    "NSE_INDEX|Nifty Bank": {"label": "bank_nifty"},
}

# Folder layout:
#   data/
#     daily/
#       2026-07-20/nifty_2026-07-20.csv        <- one folder per day, one file per instrument
#       2026-07-20/bank_nifty_2026-07-20.csv
#     nifty_ALL.csv                            <- every day's Nifty rows combined
#     bank_nifty_ALL.csv                       <- every day's Bank Nifty rows combined
BASE_DATA_DIR = "data"
DAILY_DIR = os.path.join(BASE_DATA_DIR, "daily")


def get_daily_path(label: str, date_str: str) -> str:
    """Per-day, per-instrument CSV path, always nested inside DAILY_DIR,
    e.g. data/daily/2026-07-20/nifty_2026-07-20.csv"""
    return os.path.join(DAILY_DIR, date_str, f"{label}_{date_str}.csv")


def get_validation_log_path(label: str, date_str: str) -> str:
    """Per-day, per-instrument log of every minute's cross-check against
    Upstox's official 1-minute OHLC -- a permanent, queryable record of
    exactly when/how much our reconstructed high/low diverged, instead of
    just a log line that scrolls by."""
    return os.path.join(DAILY_DIR, date_str, f"{label}_minute_validation_{date_str}.csv")


def get_combined_filename(label: str) -> str:
    return f"{label}_ALL.csv"


def get_combined_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, get_combined_filename(label))


GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Ticks are bucketed by their OWN exchange timestamp (ltt from Upstox's feed),
# NOT by local arrival time. Arrival time is skewed by network/processing
# jitter, which is exactly what caused OHLC values to mismatch Upstox's own
# per-second data for a bar with the same label. Structure:
#   tick_buckets[instrument_key][epoch_second] = [ {price, volume}, ... ]
tick_buckets = {key: {} for key in INSTRUMENTS}

# How many seconds we hold a bucket open after its second has technically
# elapsed, before finalizing/writing it. This exists ONLY to give slightly
# late-arriving ticks (still stamped with the correct ltt) time to land in
# the right bucket. Larger = more accurate vs Upstox, smaller = less delay.
BUFFER_SECONDS = 2

# The last epoch-second we've already finalized per instrument (written or
# logged as a gap). Used to walk forward one second at a time so no second
# is ever silently skipped, whether the broker sent nothing or the process stalled.
last_flushed_epoch = {key: None for key in INSTRUMENTS}

# Tracks OUR OWN reconstructed high/low per completed minute, purely so we
# can cross-check it against Upstox's own official 1-minute OHLC (delivered
# separately in the feed's marketOHLC field). Structure:
#   own_minute_stats[instrument_key][minute_epoch_sec] = {"high": .., "low": ..}
own_minute_stats = {key: {} for key in INSTRUMENTS}

# The most recent minute (epoch sec) we've already cross-checked per
# instrument, so we don't re-log the same comparison on every message.
last_validated_minute = {key: None for key in INSTRUMENTS}


def append_row_to_csv(path: str, row: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame([row])
    file_exists = os.path.isfile(path)
    df.to_csv(path, mode="a", index=False, header=not file_exists)


def write_bar(instrument_key: str, bar_time: datetime, ticks: list):
    """Writes one finalized 1s OHLCV bar for a specific instrument, built
    entirely from ticks whose own exchange timestamp (ltt) falls in this
    second -- to BOTH that day's dedicated file and that instrument's
    running all-days combined file."""
    label = INSTRUMENTS[instrument_key]["label"]
    bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")
    date_str = bar_time.strftime("%Y-%m-%d")
    tick_count = len(ticks)
    prices = [t["price"] for t in ticks]
    volumes = [t.get("volume", 0) for t in ticks]

    new_row = {
        "timestamp": bar_str,
        "instrument_key": instrument_key,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(volumes)
    }

    append_row_to_csv(get_daily_path(label, date_str), new_row)
    append_row_to_csv(get_combined_path(label), new_row)
    logger.info(f"[{label}] Saved 1s Bar -> {bar_str} IST | Close: {new_row['close']} | ticks: {tick_count}")

    # Record this bar's contribution toward its minute's running high/low,
    # so validate_minute_ohlc() can compare it against Upstox's own official
    # 1-minute OHLC once that minute closes.
    minute_epoch = (int(bar_time.timestamp()) // 60) * 60
    stats = own_minute_stats[instrument_key].setdefault(minute_epoch, {"high": new_row["high"], "low": new_row["low"]})
    stats["high"] = max(stats["high"], new_row["high"])
    stats["low"] = min(stats["low"], new_row["low"])


def flush_ready_buckets():
    """For every tracked instrument, walks forward second-by-second from the
    last finalized second up to (now - BUFFER_SECONDS), writing or gap-logging
    each one. Ticks arriving late for an already-finalized second are
    impossible by construction, since we never finalize a second until
    BUFFER_SECONDS have passed."""
    global tick_buckets, last_flushed_epoch

    now_epoch = int(time.time())
    cutoff = now_epoch - BUFFER_SECONDS  # newest second considered "settled"

    for instrument_key, label_info in INSTRUMENTS.items():
        label = label_info["label"]

        if last_flushed_epoch[instrument_key] is None:
            # This fires exactly once per instrument, at process start OR
            # restart -- there's no way to tell those apart from inside the
            # process, so we log it loudly every time. If this appears
            # mid-session (not right after the workflow/script launched),
            # it means the process itself stopped and restarted -- any
            # seconds between the last bar written before that and now were
            # never bucketed at all and are permanently unrecoverable (no
            # ticks were buffered anywhere during that window).
            now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
            logger.warning(
                f"[{label}] Bucket tracking (re)initialized at {now_str} IST. "
                f"If this wasn't the very first startup of this run, the process "
                f"itself restarted and any data since its last bar is lost -- "
                f"check GitHub Actions run history / runner logs for a restart."
            )
            last_flushed_epoch[instrument_key] = cutoff - 1
            continue

        for epoch_sec in range(last_flushed_epoch[instrument_key] + 1, cutoff + 1):
            bar_time = datetime.fromtimestamp(epoch_sec, tz=IST)
            bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")
            ticks = tick_buckets[instrument_key].pop(epoch_sec, None)
            if not ticks:
                logger.warning(f"[{label}] NO TICKS received for bar {bar_str} IST - broker delivery gap, bar skipped")
                continue
            write_bar(instrument_key, bar_time, ticks)

        last_flushed_epoch[instrument_key] = cutoff


def _get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET
    )
    return build("drive", "v3", credentials=creds)


def get_or_create_drive_folder(service, name: str, parent_id: str = None) -> str:
    """Finds a Drive folder by name (optionally scoped to a parent folder).
    Creates it if it doesn't exist yet. Returns the folder's file ID."""
    query = f"mimeType = 'application/vnd.google-apps.folder' and name = '{name}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    folders = results.get("files", [])
    if folders:
        return folders[0]["id"]

    metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        metadata["parents"] = [parent_id]
    folder = service.files().create(body=metadata, fields="id").execute()
    logger.info(f"Created Drive folder '{name}'" + (f" inside parent {parent_id}" if parent_id else ""))
    return folder["id"]


def get_daily_drive_folder_id(service) -> str:
    """Ensures data/daily/<date>/ has a matching daily/<date>/ folder on
    Drive (shared by all instruments), creating either level if missing,
    and returns the date folder's ID."""
    date_str = datetime.now(IST).strftime("%Y-%m-%d")
    daily_folder_id = get_or_create_drive_folder(service, "daily")
    date_folder_id = get_or_create_drive_folder(service, date_str, parent_id=daily_folder_id)
    return date_folder_id


def _is_combined_filename(drive_filename: str) -> bool:
    return drive_filename in {get_combined_filename(info["label"]) for info in INSTRUMENTS.values()}


def upload_file_to_drive(local_path: str, drive_filename: str, parent_id: str = None):
    """Uploads/replaces a single named file on Drive. If parent_id is given,
    the file is created/matched inside that folder instead of Drive's root.
    Daily files auto-resolve into daily/<date>/; combined files stay at root."""
    if not os.path.exists(local_path):
        return
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        if parent_id is None and not _is_combined_filename(drive_filename):
            parent_id = get_daily_drive_folder_id(service)

        query = f"name = '{drive_filename}' and trashed = false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
        if files:
            file_id = files[0]["id"]
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {"name": drive_filename}
            if parent_id:
                file_metadata["parents"] = [parent_id]
            service.files().create(body=file_metadata, media_body=media).execute()
    except Exception as e:
        logger.error(f"Google Drive Sync Failure ({drive_filename}): {e}")


def download_file_from_drive(drive_filename: str, local_path: str, parent_id: str = None):
    """Pulls an existing Drive file down to local_path if one exists. Used at
    startup so history isn't lost -- GitHub Actions runners start with an
    empty disk every run, so without this, each new run would overwrite the
    combined file (and a same-day restart would overwrite that day's file)
    with just the current run's data instead of adding to it."""
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        if parent_id is None and not _is_combined_filename(drive_filename):
            parent_id = get_daily_drive_folder_id(service)

        query = f"name = '{drive_filename}' and trashed = false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        if not files:
            return
        file_id = files[0]["id"]
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        with open(local_path, "wb") as f:
            f.write(buf.getvalue())
        logger.info(f"Resumed existing '{drive_filename}' from Drive -> {local_path}")
    except Exception as e:
        logger.error(f"Google Drive Resume/Download Failure ({drive_filename}): {e}")


async def seconds_timer_loop():
    """Periodically triggers flush_ready_buckets(). Wake-time precision no
    longer matters for correctness -- bars are labeled and bucketed using
    each tick's own exchange timestamp (ltt), not this loop's timing. This
    loop just needs to run roughly once a second so buckets don't pile up."""
    while True:
        now = time.time()
        sleep_time = 1.0 - (now % 1.0)
        await asyncio.sleep(sleep_time)
        flush_ready_buckets()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        # All uploads are blocking HTTP calls -- run each on a worker thread
        # so they can never freeze seconds_timer_loop (that's what caused
        # entire 1s bars to go missing right after every sync, before).
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        for info in INSTRUMENTS.values():
            label = info["label"]
            daily_path = get_daily_path(label, today_str)
            daily_drive_name = os.path.basename(daily_path)
            await asyncio.to_thread(upload_file_to_drive, daily_path, daily_drive_name)
            await asyncio.to_thread(upload_file_to_drive, get_combined_path(label), get_combined_filename(label))

            validation_path = get_validation_log_path(label, today_str)
            validation_drive_name = os.path.basename(validation_path)
            await asyncio.to_thread(upload_file_to_drive, validation_path, validation_drive_name)


def on_open():
    logger.info("Successfully established connection to Upstox Market Stream Feed.")


def validate_minute_ohlc(instrument_key: str, ohlc_list: list):
    """Cross-checks our own reconstructed high/low for each just-closed
    minute against Upstox's own official 1-minute OHLC, delivered
    separately in the same feed message's marketOHLC field. This is a
    diagnostic ONLY -- it never edits our stored data, it just tells you
    plainly when/why a mismatch happened (almost always a missed tick at
    the exact moment of the true high or low)."""
    global last_validated_minute, own_minute_stats

    label = INSTRUMENTS[instrument_key]["label"]
    now_minute = (int(time.time()) // 60) * 60
    TOLERANCE = 0.05  # index points; anything beyond this is a real mismatch, not rounding noise

    for entry in ohlc_list:
        if entry.get("interval") != "I1":
            continue
        try:
            entry_ts_sec = int(entry["ts"]) // 1000
        except (KeyError, TypeError, ValueError):
            continue

        if entry_ts_sec >= now_minute:
            continue  # this is the currently-forming minute, not final yet
        if last_validated_minute[instrument_key] is not None and entry_ts_sec <= last_validated_minute[instrument_key]:
            continue  # already validated this minute

        minute_str = datetime.fromtimestamp(entry_ts_sec, tz=IST).strftime("%Y-%m-%d %H:%M")
        date_str = minute_str.split(" ")[0]
        own_stats = own_minute_stats[instrument_key].get(entry_ts_sec)

        if own_stats is None:
            logger.warning(f"[{label}] Cannot cross-check minute {minute_str} IST - we recorded no bars for it locally")
        else:
            official_high = entry.get("high")
            official_low = entry.get("low")
            high_diff = round(abs(own_stats["high"] - official_high), 2) if official_high is not None else None
            low_diff = round(abs(own_stats["low"] - official_low), 2) if official_low is not None else None
            is_mismatch = (high_diff is not None and high_diff > TOLERANCE) or (low_diff is not None and low_diff > TOLERANCE)

            # Persist this minute's result either way -- a permanent audit
            # trail of exactly how often/how much this happens (e.g. at the
            # open, under high tick volatility), not just a transient log line.
            append_row_to_csv(get_validation_log_path(label, date_str), {
                "minute": minute_str,
                "our_high": own_stats["high"],
                "our_low": own_stats["low"],
                "official_high": official_high,
                "official_low": official_low,
                "high_diff": high_diff,
                "low_diff": low_diff,
                "mismatch": is_mismatch,
            })

            if is_mismatch:
                logger.warning(
                    f"[{label}] MINUTE OHLC MISMATCH vs Upstox official feed for {minute_str} IST -> "
                    f"our high={own_stats['high']} vs official high={official_high} (diff {high_diff}) | "
                    f"our low={own_stats['low']} vs official low={official_low} (diff {low_diff}) "
                    f"- likely a tick we never received at the exact extreme"
                )
            else:
                logger.info(f"[{label}] Minute {minute_str} IST OHLC matches Upstox official feed within tolerance")

        last_validated_minute[instrument_key] = entry_ts_sec
        own_minute_stats[instrument_key].pop(entry_ts_sec, None)  # prune, no longer needed


def on_message(feed_dict):
    """
    Callback invoked by MarketDataStreamerV3 for every decoded protobuf feed message.
    feed_dict is already a plain dict (via protobuf json_format.MessageToDict).
    Handles every subscribed instrument present in this message, not just one.
    """
    global tick_buckets
    try:
        feeds = feed_dict.get("feeds", {})
        for instrument_key in INSTRUMENTS:
            feed = feeds.get(instrument_key)
            if not feed:
                continue

            full_feed = feed.get("fullFeed", {})

            # NSE_INDEX instruments populate indexFF, not marketFF (indices have no
            # traded-quantity/market-depth fields the way equities/derivatives do).
            ltpc = full_feed.get("indexFF", {}).get("ltpc") \
                or full_feed.get("marketFF", {}).get("ltpc") \
                or feed.get("ltpc")

            if not ltpc:
                continue

            ltp = ltpc.get("ltp")
            ltq = ltpc.get("ltq", 0)
            ltt = ltpc.get("ltt")  # exchange timestamp of this trade, epoch millis

            if ltp is None:
                continue

            # Bucket by the tick's OWN exchange second, not local receipt time.
            if ltt is not None:
                try:
                    tick_epoch_sec = int(ltt) // 1000
                except (TypeError, ValueError):
                    tick_epoch_sec = int(time.time())
                    logger.warning(f"[{instrument_key}] ltt field unparseable ({ltt!r}) - falling back to local arrival time for this tick")
            else:
                tick_epoch_sec = int(time.time())
                logger.warning(f"[{instrument_key}] ltt field missing from tick - falling back to local arrival time for this tick")

            tick_buckets[instrument_key].setdefault(tick_epoch_sec, []).append({
                "price": float(ltp),
                "volume": float(ltq)
            })

            # Cross-check against Upstox's own official 1-minute OHLC, if present
            # in this message (separate from the ltpc tick data above).
            market_ohlc = full_feed.get("indexFF", {}).get("marketOHLC") \
                or full_feed.get("marketFF", {}).get("marketOHLC")
            if market_ohlc and market_ohlc.get("ohlc"):
                validate_minute_ohlc(instrument_key, market_ohlc["ohlc"])
    except Exception as e:
        logger.error(f"Error reading live feed update: {e}")


def on_error(error):
    logger.error(f"Upstox WebSocket Feed Error: {error}")


def on_close(close_status_code, close_msg):
    logger.info(f"WebSocket connection closed. Code: {close_status_code}, Msg: {close_msg}")


def start_streamer(token):
    """Configures and connects the MarketDataStreamerV3 (non-blocking; runs on its own thread)."""
    configuration = upstox_client.Configuration()
    configuration.access_token = token
    api_client = upstox_client.ApiClient(configuration)

    streamer = MarketDataStreamerV3(api_client, list(INSTRUMENTS.keys()), "full")
    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)

    streamer.connect()  # spawns its own background thread; returns immediately
    return streamer


async def main():
    if not UPSTOX_ACCESS_TOKEN:
        logger.error("UPSTOX_ACCESS_TOKEN configuration secret is missing!")
        sys.exit(1)

    os.makedirs(BASE_DATA_DIR, exist_ok=True)

    # Resume any existing history from Drive before appending anything new.
    # Without this, a fresh GitHub Actions runner would locally start both
    # files empty and the next sync would overwrite Drive's copies too.
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    for info in INSTRUMENTS.values():
        label = info["label"]
        daily_path = get_daily_path(label, today_str)
        await asyncio.to_thread(download_file_from_drive, os.path.basename(daily_path), daily_path)
        await asyncio.to_thread(download_file_from_drive, get_combined_filename(label), get_combined_path(label))

        validation_path = get_validation_log_path(label, today_str)
        await asyncio.to_thread(download_file_from_drive, os.path.basename(validation_path), validation_path)

    start_streamer(UPSTOX_ACCESS_TOKEN)

    logger.info("Starting up active 1s real-time loops (timestamps in IST) for: " + ", ".join(INSTRUMENTS.keys()))
    await asyncio.gather(
        seconds_timer_loop(),
        google_drive_sync_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script stopped manually by user.")
