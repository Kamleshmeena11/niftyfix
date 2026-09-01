import os
import sys
import time
import json
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import pyotp

# Fyers SDK (v3)
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# Google Drive
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Logging ---
sys.stdout.reconfigure(line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# --- Fyers app credentials (from your Fyers API app) ---
FYERS_APP_ID = os.environ.get("FYERS_APP_ID")
FYERS_APP_TYPE = os.environ.get("FYERS_APP_TYPE", "100")
FYERS_SECRET_KEY = os.environ.get("FYERS_SECRET_KEY")
FYERS_REDIRECT_URI = os.environ.get("FYERS_REDIRECT_URI")

# --- Fyers login credentials (used ONLY for automatic TOTP-based token generation) ---
FYERS_FY_ID = os.environ.get("FYERS_FY_ID")            # Your Fyers client ID, e.g. "XY01234"
FYERS_TOTP_SECRET = os.environ.get("FYERS_TOTP_SECRET")  # Base32 TOTP secret from Fyers 2FA setup
FYERS_PIN = os.environ.get("FYERS_PIN")                 # 4-digit trading PIN

# Optional: a manually-supplied token as a fallback / override
FYERS_ACCESS_TOKEN_ENV = os.environ.get("FYERS_ACCESS_TOKEN")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

TOKEN_CACHE_PATH = "fyers_token_cache.json"

INSTRUMENTS = {
    "NSE:NIFTY50-INDEX": {"label": "nifty"},
    "NSE:NIFTYBANK-INDEX": {"label": "bank_nifty"},
}

BASE_DATA_DIR = "data"
DAILY_DIR = os.path.join(BASE_DATA_DIR, "daily")

tick_buckets = {key: {} for key in INSTRUMENTS}
BUFFER_SECONDS = 2
last_flushed_epoch = {key: None for key in INSTRUMENTS}


# =========================================================
# --- Automatic Fyers token generation (TOTP login flow) ---
# =========================================================
def _load_cached_token() -> str | None:
    """Return a cached access token if it was generated today (IST)."""
    if not os.path.isfile(TOKEN_CACHE_PATH):
        return None
    try:
        with open(TOKEN_CACHE_PATH, "r") as f:
            cache = json.load(f)
        today_str = datetime.now(IST).strftime("%Y-%m-%d")
        if cache.get("date") == today_str and cache.get("access_token"):
            return cache["access_token"]
    except Exception as e:
        logger.warning(f"Could not read token cache: {e}")
    return None


def _save_token_cache(access_token: str):
    try:
        with open(TOKEN_CACHE_PATH, "w") as f:
            json.dump({
                "date": datetime.now(IST).strftime("%Y-%m-%d"),
                "access_token": access_token
            }, f)
    except Exception as e:
        logger.warning(f"Could not save token cache: {e}")


def generate_access_token_via_totp() -> str:
    """
    Fully automated Fyers login using client ID + TOTP secret + PIN.
    Requires FYERS_FY_ID, FYERS_TOTP_SECRET, FYERS_PIN, FYERS_APP_ID,
    FYERS_SECRET_KEY and FYERS_REDIRECT_URI to be set.
    """
    required = {
        "FYERS_FY_ID": FYERS_FY_ID,
        "FYERS_TOTP_SECRET": FYERS_TOTP_SECRET,
        "FYERS_PIN": FYERS_PIN,
        "FYERS_APP_ID": FYERS_APP_ID,
        "FYERS_SECRET_KEY": FYERS_SECRET_KEY,
        "FYERS_REDIRECT_URI": FYERS_REDIRECT_URI,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(
            f"Cannot auto-generate Fyers token, missing env vars: {', '.join(missing)}"
        )

    session = requests.Session()
    base = "https://api-t2.fyers.in/vagator/v2"

    # Step 1: send login OTP request for the client id
    r1 = session.post(f"{base}/send_login_otp", json={
        "fy_id": FYERS_FY_ID,
        "app_id": "2"
    })
    r1.raise_for_status()
    request_key = r1.json()["request_key"]

    # Step 2: verify TOTP
    totp_code = pyotp.TOTP(FYERS_TOTP_SECRET).now()
    r2 = session.post(f"{base}/verify_otp", json={
        "request_key": request_key,
        "otp": totp_code
    })
    r2.raise_for_status()
    request_key_2 = r2.json()["request_key"]

    # Step 3: verify PIN, get an internal session token
    r3 = session.post(f"{base}/verify_pin", json={
        "request_key": request_key_2,
        "identity_type": "pin",
        "identifier": FYERS_PIN
    })
    r3.raise_for_status()
    internal_token = r3.json()["data"]["access_token"]

    # Step 4: exchange internal session token for an auth_code against your app
    headers = {"Authorization": f"Bearer {internal_token}"}
    app_id_hash = fyersModel.SessionModel(
        client_id=f"{FYERS_APP_ID}-{FYERS_APP_TYPE}",
        secret_key=FYERS_SECRET_KEY,
        redirect_uri=FYERS_REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code"
    ).appIdHash if hasattr(fyersModel.SessionModel, "appIdHash") else None

    token_payload = {
        "fyers_id": FYERS_FY_ID,
        "app_id": FYERS_APP_ID,
        "redirect_uri": FYERS_REDIRECT_URI,
        "appType": FYERS_APP_TYPE,
        "code_challenge": "",
        "state": "state",
        "scope": "",
        "nonce": "",
        "response_type": "code",
        "create_cookie": True
    }
    r4 = session.post("https://api-t2.fyers.in/api/v3/token", json=token_payload, headers=headers)
    r4.raise_for_status()
    r4_data = r4.json()
    redirect_url = r4_data.get("Url") or r4_data.get("url")
    if not redirect_url or "auth_code=" not in redirect_url:
        raise RuntimeError(f"Unexpected response during auth_code exchange: {r4_data}")
    auth_code = redirect_url.split("auth_code=")[1].split("&")[0]

    # Step 5: exchange auth_code for the final access token via the SDK
    session_model = fyersModel.SessionModel(
        client_id=f"{FYERS_APP_ID}-{FYERS_APP_TYPE}",
        secret_key=FYERS_SECRET_KEY,
        redirect_uri=FYERS_REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code"
    )
    session_model.set_token(auth_code)
    response = session_model.generate_token()

    if "access_token" not in response:
        raise RuntimeError(f"Token generation failed: {response}")

    access_token = response["access_token"]
    _save_token_cache(access_token)
    logger.info("Fyers access token generated automatically via TOTP login.")
    return access_token


def validate_token(access_token: str) -> bool:
    """Confirm the token actually works before opening the websocket."""
    try:
        client = fyersModel.FyersModel(
            client_id=f"{FYERS_APP_ID}-{FYERS_APP_TYPE}",
            token=access_token,
            is_async=False,
            log_path=""
        )
        profile = client.get_profile()
        return profile.get("s") == "ok"
    except Exception as e:
        logger.warning(f"Token validation failed: {e}")
        return False


def get_valid_access_token() -> str:
    """
    Resolution order:
    1. Cached token from today, if still valid.
    2. FYERS_ACCESS_TOKEN env var, if still valid.
    3. Auto-generate via TOTP login.
    """
    cached = _load_cached_token()
    if cached and validate_token(cached):
        logger.info("Using cached Fyers access token.")
        return cached

    if FYERS_ACCESS_TOKEN_ENV and validate_token(FYERS_ACCESS_TOKEN_ENV):
        logger.info("Using FYERS_ACCESS_TOKEN from environment.")
        _save_token_cache(FYERS_ACCESS_TOKEN_ENV)
        return FYERS_ACCESS_TOKEN_ENV

    logger.info("No valid token found — generating a new one automatically.")
    return generate_access_token_via_totp()


# --- Bar writing helpers ---
def get_daily_path(label: str, date_str: str) -> str:
    return os.path.join(DAILY_DIR, date_str, f"{label}_{date_str}.csv")


def get_combined_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, f"{label}_ALL.csv")


def append_row_to_csv(path: str, row: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame([row])
    file_exists = os.path.isfile(path)
    df.to_csv(path, mode="a", index=False, header=not file_exists)


def write_bar(instrument_key: str, bar_time: datetime, ticks: list):
    label = INSTRUMENTS[instrument_key]["label"]
    bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")
    date_str = bar_time.strftime("%Y-%m-%d")
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
    logger.info(f"[{label}] Saved 1s Bar -> {bar_str} IST | Close: {new_row['close']} | Ticks: {len(ticks)}")


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
            ticks = tick_buckets[instrument_key].pop(epoch_sec, None)
            if not ticks:
                continue
            write_bar(instrument_key, bar_time, ticks)

        last_flushed_epoch[instrument_key] = cutoff


# --- Google Drive helpers ---
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


# --- Fyers WebSocket callbacks ---
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
    # If the token itself is rejected mid-stream, wipe the cache so the
    # next connection attempt is forced to regenerate a fresh one.
    if isinstance(message, dict) and message.get("code") == -300:
        logger.warning("Token rejected by server — clearing cache for regeneration.")
        try:
            if os.path.isfile(TOKEN_CACHE_PATH):
                os.remove(TOKEN_CACHE_PATH)
        except Exception:
            pass


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


async def run_websocket_with_retry():
    """Keep the data socket alive, regenerating the token if the server
    rejects it and reconnecting with backoff instead of dying silently."""
    backoff = 5
    while True:
        try:
            access_token = get_valid_access_token()
            app_id_full = f"{FYERS_APP_ID}-{FYERS_APP_TYPE}"

            fyers_ws = data_ws.FyersDataSocket(
                access_token=f"{app_id_full}:{access_token}",
                log_path="",
                litemode=False,
                write_to_file=False,
                reconnect=True,
                on_connect=lambda: on_open(fyers_ws),
                on_close=on_close,
                on_error=on_error,
                on_message=on_message
            )
            fyers_ws.connect()
            backoff = 5  # reset after a successful connect
            # The SDK's own thread drives the socket; just idle here and
            # let seconds_timer_loop / google_drive_sync_loop keep running.
            while True:
                await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"WebSocket loop crashed: {e} — retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def main():
    os.makedirs(BASE_DATA_DIR, exist_ok=True)

    logger.info("Starting real-time data loops...")
    await asyncio.gather(
        run_websocket_with_retry(),
        seconds_timer_loop(),
        google_drive_sync_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Collector stopped manually.")
