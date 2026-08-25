import os
import sys
import time
import logging
import asyncio
import pyotp
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd

# Fyers SDK
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersDataSocket import data_ws

# Google Drive Modules
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials

# --- Logging Setup ---
sys.stdout.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Timezone ---
IST = ZoneInfo("Asia/Kolkata")

# --- Environment Variables ---
FYERS_APP_ID = os.environ.get("FYERS_APP_ID")
FYERS_APP_TYPE = os.environ.get("FYERS_APP_TYPE", "100")
FYERS_SECRET_KEY = os.environ.get("FYERS_SECRET_KEY")
FYERS_FY_ID = os.environ.get("FYERS_FY_ID")
FYERS_TOTP_SECRET = os.environ.get("FYERS_TOTP_SECRET")
FYERS_PIN = os.environ.get("FYERS_PIN")
FYERS_REDIRECT_URI = os.environ.get("FYERS_REDIRECT_URI", "https://trade.fyers.in/api-login/default-redirect-uri/index.php")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Instruments mapping for Fyers API v3
INSTRUMENTS = {
    "NSE:NIFTY50-INDEX": {"label": "nifty"},
    "NSE:NIFTYBANK-INDEX": {"label": "bank_nifty"},
}

BASE_DATA_DIR = "data"
DAILY_DIR = os.path.join(BASE_DATA_DIR, "daily")

tick_buckets = {key: {} for key in INSTRUMENTS}
BUFFER_SECONDS = 2
last_flushed_epoch = {key: None for key in INSTRUMENTS}


def get_daily_path(label: str, date_str: str) -> str:
    return os.path.join(DAILY_DIR, date_str, f"{label}_{date_str}.csv")


def get_combined_filename(label: str) -> str:
    return f"{label}_ALL.csv"


def get_combined_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, get_combined_filename(label))


def append_row_to_csv(path: str, row: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame([row])
    file_exists = os.path.isfile(path)
    df.to_csv(path, mode="a", index=False, header=not file_exists)


def write_bar(instrument_key: str, bar_time: datetime, ticks: list):
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
    logger.info(f"[{label}] Saved 1s Bar -> {bar_str} IST | Close: {new_row['close']} | Ticks: {tick_count}")


def flush_ready_buckets():
    global tick_buckets, last_flushed_epoch

    now_epoch = int(time.time())
    cutoff = now_epoch - BUFFER_SECONDS

    for instrument_key, label_info in INSTRUMENTS.items():
        label = label_info["label"]

        if last_flushed_epoch[instrument_key] is None:
            now_str = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
            logger.warning(f"[{label}] Bucket tracking initialized at {now_str} IST.")
            last_flushed_epoch[instrument_key] = cutoff - 1
            continue

        for epoch_sec in range(last_flushed_epoch[instrument_key] + 1, cutoff + 1):
            bar_time = datetime.fromtimestamp(epoch_sec, tz=IST)
            bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")
            ticks = tick_buckets[instrument_key].pop(epoch_sec, None)
            if not ticks:
                continue
            write_bar(instrument_key, bar_time, ticks)

        last_flushed_epoch[instrument_key] = cutoff


# --- Google Drive Helpers ---
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
    return folder["id"]


def upload_file_to_drive(local_path: str, drive_filename: str):
    if not os.path.exists(local_path) or not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        daily_folder_id = get_or_create_drive_folder(service, "daily")
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        parent_id = get_or_create_drive_folder(service, today_str, parent_id=daily_folder_id)

        query = f"name = '{drive_filename}' and trashed = false and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)

        if files:
            service.files().update(fileId=files[0]["id"], media_body=media).execute()
        else:
            service.files().create(body={"name": drive_filename, "parents": [parent_id]}, media_body=media).execute()
    except Exception as e:
        logger.error(f"Google Drive Upload Error ({drive_filename}): {e}")


# --- Fyers Authentication ---
def get_fyers_access_token():
    logger.info("Generating Fyers access token via TOTP...")
    app_id_full = f"{FYERS_APP_ID}-{FYERS_APP_TYPE}"
    
    session = fyersModel.SessionModel(
        client_id=app_id_full,
        secret_key=FYERS_SECRET_KEY,
        redirect_uri=FYERS_REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code"
    )
    
    totp = pyotp.TOTP(FYERS_TOTP_SECRET).now()
    
    # Auto-login step (requires valid credentials)
    login_response = session.generate_authcode()
    # Note: If automatic auth-code generation is configured on your Fyers app,
    # supply access token directly via FYERS_ACCESS_TOKEN or run local token generation.
    return os.environ.get("FYERS_ACCESS_TOKEN", login_response)


# --- Fyers WebSocket Streamer ---
def on_message(message):
    try:
        symbol = message.get("symbol")
        ltp = message.get("ltp")
        if symbol in tick_buckets and ltp is not None:
            ltt = message.get("ltt", int(time.time()))
            tick_epoch_sec = int(ltt)
            tick_buckets[symbol].setdefault(tick_epoch_sec, []).append({
                "price": float(ltp),
                "volume": float(message.get("vol_traded_today", 0))
            })
    except Exception as e:
        logger.error(f"Error parsing feed message: {e}")


def on_error(message):
    logger.error(f"Fyers WebSocket Error: {message}")


def on_close(message):
    logger.info(f"Fyers WebSocket Connection Closed: {message}")


def on_open(fyers_socket):
    logger.info("Connected to Fyers Data Socket. Subscribing to symbols...")
    symbols = list(INSTRUMENTS.keys())
    fyers_socket.subscribe(symbols=symbols, data_type="SymbolUpdate")
    fyers_socket.keep_running()


async def seconds_timer_loop():
    while True:
        now = time.time()
        await asyncio.sleep(1.0 - (now % 1.0))
        flush_ready_buckets()


async def google_drive_sync_loop():
    while True:
        await asyncio.sleep(10)
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        for info in INSTRUMENTS.values():
            label = info["label"]
            daily_path = get_daily_path(label, today_str)
            await asyncio.to_thread(upload_file_to_drive, daily_path, os.path.basename(daily_path))


async def main():
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    access_token = os.environ.get("FYERS_ACCESS_TOKEN") or get_fyers_access_token()
    app_id_full = f"{FYERS_APP_ID}-{FYERS_APP_TYPE}"

    fyers_ws = data_ws.FyersDataSocket(
        access_token=f"{app_id_full}:{access_token}",
        log_path="",
        lStream=True,
        on_connect=lambda: on_open(fyers_ws),
        on_close=on_close,
        on_error=on_error,
        on_message=on_message
    )
    
    fyers_ws.connect()

    logger.info("Starting real-time data loops...")
    await asyncio.gather(
        seconds_timer_loop(),
        google_drive_sync_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Collector stopped manually.")
