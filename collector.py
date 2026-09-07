"""
fyers_execution_debug.py

Standalone diagnostic -- NOT the full collector. Subscribes only to
SymbolUpdate (trade prints) for one symbol and, for every trade, logs the
classified execution side (BUY vs SELL) using the exact same logic just
applied to the fixed collector: reading bid_price/ask_price straight off
each trade message (confirmed present via your earlier raw debug capture,
e.g. {'bid_price': 2282.8, 'ask_price': 2283.0, ...}) rather than depending
on a separately-tracked depth/TBT feed.

It also keeps a running BUY/SELL tally printed on every line, so instead
of scrolling through hundreds of raw ticks trying to eyeball whether sells
ever show up, you can just watch the tally at the end of each line grow on
both sides (or not).

USAGE: same env vars as the other scripts:
    FYERS_APP_ID, FYERS_APP_TYPE (default "100"), FYERS_SECRET_KEY,
    FYERS_REDIRECT_URI, FYERS_FY_ID, FYERS_TOTP_SECRET, FYERS_PIN
    (for automatic TOTP login), OR just FYERS_ACCESS_TOKEN if you already
    have a valid token.
    FYERS_SYMBOL (default "NSE:TCS-EQ")

    python fyers_execution_debug.py

Reuses the same "fyers_token_cache.json" as the other scripts, so if one
of them already logged in today from the same working directory, this
just reuses that cached token.

WHAT TO LOOK FOR:
    Each line looks like:
        [EXEC 42] 09:41:07 SIDE=SELL price=2282.8 bid=2282.8 ask=2283.0
        delta_vol=3 last_traded_qty=3 | tally BUY=25 SELL=17

    - If SELL never appears (tally stays 0) even though `bid` and `ask`
      are clearly two different real numbers and `price` is sometimes
      equal to or below `bid`, something's still off in the classification
      itself -- paste back a stretch of this output and we'll dig further.
    - If `bid`/`ask` ever print as the SAME value as `price` (rather than
      independent numbers), that means this particular message didn't
      carry bid_price/ask_price and we fell back to stale/absent state --
      also worth flagging.
    - `delta_vol` (computed from the cumulative vol_traded_today field, the
      same way the main collector does it) vs `last_traded_qty` (a field
      Fyers sends directly, which the main collector's comments assumed
      wasn't reliably present) -- if these two numbers regularly disagree,
      that's a separate volume-accuracy issue worth fixing too, just not
      the one we're chasing right now.
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


# --- Token handling (same logic as the other scripts, trimmed) ---

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


# --- Execution classification + tally ---

_state = {
    "last_cum_volume": None,
    "last_side": "BUY",
    "best_bid": None,
    "best_ask": None,
}
_counts = {"BUY": 0, "SELL": 0}
_trade_num = 0


def handle_trade(message: dict):
    global _trade_num

    ltp = message.get("ltp")
    if ltp is None:
        return

    cum_vol = message.get("vol_traded_today")
    if cum_vol is None:
        return
    try:
        cum_vol = float(cum_vol)
    except (TypeError, ValueError):
        return

    prev_cum = _state["last_cum_volume"]
    _state["last_cum_volume"] = cum_vol
    if prev_cum is None:
        return  # first message just establishes the baseline
    delta_size = cum_vol - prev_cum
    if delta_size <= 0:
        return  # no new trade since the last message

    # Prefer the quote embedded directly on this trade message; fall back
    # to whatever we last saw if this particular message omits it.
    bid_price = message.get("bid_price")
    ask_price = message.get("ask_price")
    if ask_price is not None:
        _state["best_ask"] = ask_price
    if bid_price is not None:
        _state["best_bid"] = bid_price

    best_ask = _state["best_ask"]
    best_bid = _state["best_bid"]

    if best_ask is not None and ltp >= best_ask:
        side = "BUY"
    elif best_bid is not None and ltp <= best_bid:
        side = "SELL"
    else:
        side = _state["last_side"]
    _state["last_side"] = side
    _counts[side] += 1
    _trade_num += 1

    ltq = message.get("last_traded_qty")
    now_str = datetime.now(IST).strftime("%H:%M:%S")
    logger.info(
        f"[EXEC {_trade_num}] {now_str} SIDE={side:<4} price={ltp} "
        f"bid={best_bid} ask={best_ask} delta_vol={delta_size:g} last_traded_qty={ltq} "
        f"| tally BUY={_counts['BUY']} SELL={_counts['SELL']}"
    )


def on_message(message):
    if not isinstance(message, dict):
        return
    if "ltp" in message:
        handle_trade(message)
    # Depth messages are ignored entirely in this script -- execution side
    # is fully determined from the trade message's own bid_price/ask_price.


def on_error(message):
    logger.error(f"WebSocket Error: {message}")


def on_close(message):
    logger.info(f"WebSocket Closed: {message}")


def on_open(fyers_socket):
    logger.info(f"Connected. Subscribing to {FYERS_SYMBOL} (SymbolUpdate only)...")
    fyers_socket.subscribe(symbols=[FYERS_SYMBOL], data_type="SymbolUpdate")
    fyers_socket.keep_running()


def main():
    access_token = get_valid_access_token()
    app_id_full = f"{FYERS_APP_ID}-{FYERS_APP_TYPE}"

    logger.info(f"Starting execution-side debug for {FYERS_SYMBOL} — watch the BUY/SELL tally on each line (Ctrl+C to stop).")

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
