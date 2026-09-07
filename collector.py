"""
fyers_depth_debug.py

Minimal, standalone diagnostic script -- NOT the full collector. It does
nothing except log in, subscribe to one symbol's SymbolUpdate + DepthUpdate
feeds, and print every raw message it receives, completely unparsed. No
Google Drive, no CSV writing, no 1-second bars -- just enough to see exactly
what Fyers is actually sending for depth, so extract_depth_levels() in the
real collector can be fixed to match instead of guessing at field names.

USAGE:
    Set the same env vars you already use for the main collector (whichever
    subset applies):
        FYERS_APP_ID, FYERS_APP_TYPE (default "100"), FYERS_SECRET_KEY,
        FYERS_REDIRECT_URI, FYERS_FY_ID, FYERS_TOTP_SECRET, FYERS_PIN
        (for automatic TOTP login), OR just FYERS_ACCESS_TOKEN if you
        already have a valid token.
        FYERS_SYMBOL (default "NSE:TCS-EQ")
        DEBUG_MAX_MESSAGES (default 40) -- how many DEPTH messages to print
        before exiting. Trade/quote messages are printed too but don't
        count against this limit, since they're not what we're debugging.

    python fyers_depth_debug.py

It reuses the same token_cache file ("fyers_token_cache.json") as the main
collector, so if that's already been run today from the same working
directory, this will just reuse the cached token -- no extra login.

WHAT TO LOOK FOR:
    Each depth message prints a "keys=" summary line first (which of the
    known ask/bid field names are present) followed by the full raw dict.
    Paste back a handful of these -- ideally including at least one where
    "keys=" shows both ask-ish and bid-ish fields, and a few in a row so we
    can see whether Fyers sends one full two-sided snapshot per message, or
    alternates full snapshots per side, or sends true incremental deltas.
"""

import os
import sys
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pyotp

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

sys.stdout.reconfigure(line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
TOKEN_CACHE_PATH = "fyers_token_cache.json"


def _normalize_app_id(raw: str, app_type: str) -> str:
    if not raw:
        return raw
    raw = raw.strip()
    suffix = f"-{app_type}"
    if raw.endswith(suffix):
        return raw[: -len(suffix)]
    if "-" in raw:
        head, _, tail = raw.rpartition("-")
        if tail.isdigit():
            return head
    return raw


_RAW_APP_TYPE = os.environ.get("FYERS_APP_TYPE", "100")
FYERS_APP_ID = _normalize_app_id(os.environ.get("FYERS_APP_ID"), _RAW_APP_TYPE)
FYERS_APP_TYPE = _RAW_APP_TYPE
FYERS_SECRET_KEY = os.environ.get("FYERS_SECRET_KEY")
FYERS_REDIRECT_URI = os.environ.get("FYERS_REDIRECT_URI")
FYERS_FY_ID = os.environ.get("FYERS_FY_ID")
FYERS_TOTP_SECRET = os.environ.get("FYERS_TOTP_SECRET")
FYERS_PIN = os.environ.get("FYERS_PIN")
FYERS_ACCESS_TOKEN_ENV = os.environ.get("FYERS_ACCESS_TOKEN")

FYERS_SYMBOL = os.environ.get("FYERS_SYMBOL", "NSE:TCS-EQ")
DEBUG_MAX_MESSAGES = int(os.environ.get("DEBUG_MAX_MESSAGES", "40"))

_depth_message_count = 0

KNOWN_ASK_KEYS = ("ask", "asks", "ask_price1")
KNOWN_BID_KEYS = ("bid", "bids", "bid_price1")


# --- Token handling (same logic as the main collector, trimmed) ---

def _load_cached_token():
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
            json.dump({"date": datetime.now(IST).strftime("%Y-%m-%d"), "access_token": access_token}, f)
    except Exception as e:
        logger.warning(f"Could not save token cache: {e}")


def generate_access_token_via_totp() -> str:
    required = {
        "FYERS_FY_ID": FYERS_FY_ID, "FYERS_TOTP_SECRET": FYERS_TOTP_SECRET,
        "FYERS_PIN": FYERS_PIN, "FYERS_APP_ID": FYERS_APP_ID,
        "FYERS_SECRET_KEY": FYERS_SECRET_KEY, "FYERS_REDIRECT_URI": FYERS_REDIRECT_URI,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Cannot auto-generate Fyers token, missing env vars: {', '.join(missing)}")

    session = requests.Session()
    base = "https://api-t2.fyers.in/vagator/v2"

    def _post(url, payload, step_name, headers=None):
        resp = session.post(url, json=payload, headers=headers)
        if not resp.ok:
            raise RuntimeError(f"{step_name} failed ({resp.status_code}): {resp.text}")
        return resp

    r1 = _post(f"{base}/send_login_otp", {"fy_id": FYERS_FY_ID, "app_id": "2"}, "send_login_otp")
    request_key = r1.json()["request_key"]

    totp_code = pyotp.TOTP(FYERS_TOTP_SECRET).now()
    r2 = _post(f"{base}/verify_otp", {"request_key": request_key, "otp": totp_code}, "verify_otp")
    request_key_2 = r2.json()["request_key"]

    r3 = _post(f"{base}/verify_pin", {"request_key": request_key_2, "identity_type": "pin", "identifier": FYERS_PIN}, "verify_pin")
    internal_token = r3.json()["data"]["access_token"]

    headers = {"Authorization": f"Bearer {internal_token}"}
    token_payload = {
        "fyers_id": FYERS_FY_ID, "app_id": FYERS_APP_ID, "redirect_uri": FYERS_REDIRECT_URI,
        "appType": FYERS_APP_TYPE, "code_challenge": "", "state": "sample_state",
        "scope": "", "nonce": "", "response_type": "code", "create_cookie": True
    }
    r4 = session.post("https://api-t2.fyers.in/api/v3/token", json=token_payload, headers=headers, allow_redirects=False)
    if r4.status_code != 308:
        raise RuntimeError(f"Unexpected status {r4.status_code} during auth_code exchange: {r4.text}")
    r4_data = r4.json()
    redirect_url = r4_data.get("Url") or r4_data.get("url")
    if not redirect_url or "auth_code=" not in redirect_url:
        raise RuntimeError(f"Unexpected response during auth_code exchange: {r4_data}")
    auth_code = redirect_url.split("auth_code=")[1].split("&")[0]

    session_model = fyersModel.SessionModel(
        client_id=f"{FYERS_APP_ID}-{FYERS_APP_TYPE}", secret_key=FYERS_SECRET_KEY,
        redirect_uri=FYERS_REDIRECT_URI, response_type="code", grant_type="authorization_code"
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
    try:
        client = fyersModel.FyersModel(client_id=f"{FYERS_APP_ID}-{FYERS_APP_TYPE}", token=access_token, is_async=False, log_path="")
        profile = client.get_profile()
        return profile.get("s") == "ok"
    except Exception as e:
        logger.warning(f"Token validation failed: {e}")
        return False


def get_valid_access_token() -> str:
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


# --- Raw message printing ---

def _keys_summary(message: dict) -> str:
    present = [k for k in (KNOWN_ASK_KEYS + KNOWN_BID_KEYS) if k in message]
    ask_hit = any(k in message for k in KNOWN_ASK_KEYS)
    bid_hit = any(k in message for k in KNOWN_BID_KEYS)
    verdict = "ASK+BID" if (ask_hit and bid_hit) else ("ASK ONLY" if ask_hit else ("BID ONLY" if bid_hit else "NEITHER (unrecognized)"))
    all_keys = sorted(message.keys())
    return f"verdict={verdict} matched_known_keys={present} ALL_KEYS={all_keys}"


def on_message(message):
    global _depth_message_count
    if not isinstance(message, dict):
        logger.info(f"[NON-DICT MESSAGE] {message!r}")
        return

    looks_like_depth = any(k in message for k in (KNOWN_ASK_KEYS + KNOWN_BID_KEYS))

    if looks_like_depth:
        _depth_message_count += 1
        logger.info(f"[DEPTH {_depth_message_count}/{DEBUG_MAX_MESSAGES}] {_keys_summary(message)}")
        logger.info(f"[DEPTH {_depth_message_count}/{DEBUG_MAX_MESSAGES}] RAW={message}")
        if _depth_message_count >= DEBUG_MAX_MESSAGES:
            logger.info(f"Reached DEBUG_MAX_MESSAGES={DEBUG_MAX_MESSAGES}. Ctrl+C to exit.")
    elif "ltp" in message:
        logger.info(f"[TRADE] {message}")
    else:
        logger.info(f"[OTHER] {message}")


def on_error(message):
    logger.error(f"WebSocket Error: {message}")


def on_close(message):
    logger.info(f"WebSocket Closed: {message}")


def on_open(fyers_socket):
    logger.info(f"Connected. Subscribing to {FYERS_SYMBOL} (SymbolUpdate + DepthUpdate)...")
    fyers_socket.subscribe(symbols=[FYERS_SYMBOL], data_type="SymbolUpdate")
    fyers_socket.subscribe(symbols=[FYERS_SYMBOL], data_type="DepthUpdate")
    fyers_socket.keep_running()


def main():
    access_token = get_valid_access_token()
    app_id_full = f"{FYERS_APP_ID}-{FYERS_APP_TYPE}"

    logger.info(f"Starting raw depth debug for {FYERS_SYMBOL} — will log up to {DEBUG_MAX_MESSAGES} depth messages, then keep running (Ctrl+C to stop).")

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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stopped manually.")
