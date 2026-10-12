import os
import sys
import time
import json
import io
import base64
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pyotp
import websockets  # needed only when FYERS_DEPTH_SOURCE=tbt

# Fyers SDK (v3)
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# TBT (Versova) protobuf schema — only needed when FYERS_DEPTH_SOURCE=tbt.
try:
    import msg_pb2 as fyers_tbt_pb2
except ImportError:
    fyers_tbt_pb2 = None

# Google Drive
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
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


def _env_bool(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() not in ("0", "false", "no")


# --- Fyers app credentials ---
def _normalize_app_id(raw: str, app_type: str) -> str:
    """Accepts 'XY0W1234' or 'XY0W1234-100' and returns the bare app id."""
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

# --- Fyers login credentials (automatic TOTP-based token generation) ---
FYERS_FY_ID = os.environ.get("FYERS_FY_ID")
FYERS_TOTP_SECRET = os.environ.get("FYERS_TOTP_SECRET")
FYERS_PIN = os.environ.get("FYERS_PIN")

# Optional: a manually-supplied token as a fallback / override
FYERS_ACCESS_TOKEN_ENV = os.environ.get("FYERS_ACCESS_TOKEN")

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# --- GitHub (second, independent backup + fallback restore source) ---
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY")  # "owner/repo"
GITHUB_BRANCH = os.environ.get("GITHUB_DATA_BRANCH") or os.environ.get("GITHUB_REF_NAME") or "main"
GITHUB_API_BASE = "https://api.github.com"
GITHUB_SYNC_INTERVAL_SECONDS = int(os.environ.get("GITHUB_SYNC_INTERVAL_SECONDS", "120"))
# Which running files get pushed to GitHub. Keys:
#   TCS:        raw, level2, 1s
#   Bank Nifty: bn_raw, bn_1s   (OFF by default — BN tick files get large;
#                                Drive + the end-of-run artifact are better for those)
GITHUB_SYNC_FILES = {
    f.strip().lower()
    for f in os.environ.get("GITHUB_SYNC_FILES", "raw,level2,1s").split(",")
    if f.strip()
}
GITHUB_REQUEST_TIMEOUT_SECONDS = int(os.environ.get("GITHUB_REQUEST_TIMEOUT_SECONDS", "60"))
GITHUB_MAX_RETRIES = int(os.environ.get("GITHUB_MAX_RETRIES", "3"))

TOKEN_CACHE_PATH = "fyers_token_cache.json"

# =========================================================
# --- Instruments ---
# =========================================================
# 1) TCS (equity cash): Level 1 + Level 2 + 1-second bars  (UNCHANGED behavior)
# 2) Bank Nifty futures: Level 1 raw ticks only (RawData.csv), optional 1s bars
#
# Bank Nifty futures symbol format: NSE:BANKNIFTY<YY><MON>FUT, e.g.
# NSE:BANKNIFTY26OCTFUT. Contracts are monthly — UPDATE FYERS_BANKNIFTY_SYMBOL
# AT EACH EXPIRY ROLLOVER (verify the exact symbol in Fyers' symbol master).
TCS_SYMBOL = os.environ.get("FYERS_SYMBOL", "NSE:TCS-EQ")
TCS_LABEL = "tcs"
FYERS_SYMBOL = TCS_SYMBOL            # kept for backward compatibility
INSTRUMENT_LABEL = TCS_LABEL         # kept for backward compatibility

BN_ENABLED = _env_bool("FYERS_BANKNIFTY_ENABLED", "true")
BN_SYMBOL = os.environ.get("FYERS_BANKNIFTY_SYMBOL", "NSE:BANKNIFTY26OCTFUT")
BN_LABEL = os.environ.get("FYERS_BANKNIFTY_LABEL", "banknifty")
# Optional 1-second OHLCV bars for Bank Nifty (off by default: you asked for raw ticks).
BN_ONE_SECOND = _env_bool("FYERS_BANKNIFTY_1S", "false")
# Futures vol_traded_today is in UNITS (1 lot = lot-size units). Splitting into
# 1-volume rows would write dozens of rows per lot, so this defaults to false.
BN_SPLIT_PRINTS = _env_bool("FYERS_BANKNIFTY_SPLIT_PRINTS", "false")

# NOTE ON DEPTH: number of levels/side requested for TCS. extract_depth_levels()
# slices to this cap; fewer levels from Fyers just means fewer lines.
DOM_LEVELS = int(os.environ.get("FYERS_DOM_LEVELS", "50"))

# --- Depth source selection (applies to TCS only) ---
# "standard" (default): data_ws DepthUpdate feed.
# "tbt": Fyers TBT/Versova protobuf WebSocket (currently NFO-only per Fyers docs,
# so it will NOT work for NSE:TCS-EQ).
FYERS_DEPTH_SOURCE = os.environ.get("FYERS_DEPTH_SOURCE", "standard").strip().lower()
FYERS_TBT_WS_URL = os.environ.get("FYERS_TBT_WS_URL", "wss://rtsocket-api.fyers.in/versova")

# Temporary debug aid: log the first N raw DepthUpdate messages.
FYERS_DEBUG_RAW_DEPTH_MESSAGES = int(os.environ.get("FYERS_DEBUG_RAW_DEPTH_MESSAGES", "5"))
_raw_depth_debug_count = 0

# TCS: split each trade print into N separate 1-volume lines (matches NT8 1-Volume series).
FYERS_SPLIT_PRINTS = _env_bool("FYERS_SPLIT_PRINTS", "true")

# TCS: also write explicit "L1;{side};..." execution-tag lines into Level2.csv.
FYERS_WRITE_L1_TAG_LINES = _env_bool("FYERS_WRITE_L1_TAG_LINES", "true")

BASE_DATA_DIR = "data"

# Layout: ONE folder per instrument.
#   data/tcs/RawData.csv, data/tcs/Level2.csv, data/tcs/1Second_tcs.csv
#   data/banknifty/RawData.csv  (+ 1Second_banknifty.csv if enabled)
RAW_DATA_FILENAME = "RawData.csv"
LEVEL2_FILENAME = "Level2.csv"
ONE_SECOND_FILENAME = "1Second_tcs.csv"          # TCS (unchanged name)
BN_ONE_SECOND_FILENAME = f"1Second_{BN_LABEL}.csv"

RAW_DATA_HEADER = "Timestamp;Price;Bid;Ask;Volume"
ONE_SECOND_HEADER = "timestamp,instrument_key,open,high,low,close,volume"

ONE_SECOND_BUFFER_SECONDS = int(os.environ.get("FYERS_1S_BUFFER_SECONDS", "2"))

# Drive sync cadence (seconds). Each sync re-uploads the whole file, and the
# Bank Nifty tick file grows much faster than TCS, so it syncs less often.
DRIVE_SYNC_INTERVAL_SECONDS = int(os.environ.get("DRIVE_SYNC_INTERVAL_SECONDS", "10"))
BN_DRIVE_SYNC_INTERVAL_SECONDS = int(os.environ.get("BN_DRIVE_SYNC_INTERVAL_SECONDS", "30"))

# Per-instrument config, keyed by Fyers symbol.
INSTRUMENTS = {
    TCS_SYMBOL: {
        "label": TCS_LABEL,
        "depth": True,                       # Level 2 enabled
        "l1_tags": FYERS_WRITE_L1_TAG_LINES,
        "split": FYERS_SPLIT_PRINTS,
        "one_second": True,
        "one_second_filename": ONE_SECOND_FILENAME,
    },
}
if BN_ENABLED:
    INSTRUMENTS[BN_SYMBOL] = {
        "label": BN_LABEL,
        "depth": False,                      # Level 1 only
        "l1_tags": False,                    # no Level2.csv for Bank Nifty
        "split": BN_SPLIT_PRINTS,
        "one_second": BN_ONE_SECOND,
        "one_second_filename": BN_ONE_SECOND_FILENAME,
    }


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
    """Fully automated Fyers login using client ID + TOTP secret + PIN."""
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

    def _mask(v: str) -> str:
        v = v.strip()
        if len(v) <= 4:
            return "*" * len(v)
        return f"{v[:2]}***{v[-2:]} (len={len(v)})"

    logger.info(
        f"Using FYERS_APP_ID={_mask(FYERS_APP_ID)} (normalized, bare app id) "
        f"FYERS_APP_TYPE={FYERS_APP_TYPE!r} "
        f"FYERS_REDIRECT_URI={FYERS_REDIRECT_URI!r}"
    )

    session = requests.Session()
    base = "https://api-t2.fyers.in/vagator/v2"

    def _post(url: str, payload: dict, step_name: str, headers: dict = None):
        resp = session.post(url, json=payload, headers=headers)
        if not resp.ok:
            raise RuntimeError(f"{step_name} failed ({resp.status_code}): {resp.text}")
        return resp

    # Step 1: send login OTP request
    r1 = _post(f"{base}/send_login_otp", {"fy_id": FYERS_FY_ID, "app_id": "2"}, "send_login_otp")
    request_key = r1.json()["request_key"]

    # Step 2: verify TOTP
    totp_code = pyotp.TOTP(FYERS_TOTP_SECRET).now()
    r2 = _post(f"{base}/verify_otp", {"request_key": request_key, "otp": totp_code}, "verify_otp")
    request_key_2 = r2.json()["request_key"]

    # Step 3: verify PIN
    r3 = _post(f"{base}/verify_pin", {
        "request_key": request_key_2,
        "identity_type": "pin",
        "identifier": FYERS_PIN
    }, "verify_pin")
    internal_token = r3.json()["data"]["access_token"]

    # Step 4: exchange internal session token for an auth_code
    headers = {"Authorization": f"Bearer {internal_token}"}
    token_payload = {
        "fyers_id": FYERS_FY_ID,
        "app_id": FYERS_APP_ID,
        "redirect_uri": FYERS_REDIRECT_URI,
        "appType": FYERS_APP_TYPE,
        "code_challenge": "",
        "state": "sample_state",
        "scope": "",
        "nonce": "",
        "response_type": "code",
        "create_cookie": True
    }
    # This endpoint returns HTTP 308 on success with the redirect URL in the
    # JSON body — it must NOT be auto-followed.
    r4 = session.post(
        "https://api-t2.fyers.in/api/v3/token",
        json=token_payload,
        headers=headers,
        allow_redirects=False
    )
    if r4.status_code != 308:
        raise RuntimeError(
            f"Unexpected status {r4.status_code} during auth_code exchange: {r4.text}"
        )
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


# =========================================================
# --- L1/L2 output ---
# =========================================================
#   RawData.csv : Timestamp;Price;Bid;Ask;Volume  (one row per trade/tick)
#   L1 line:  L1;{side};{yyyyMMddHHmmss};{ffffff};{price};{size}      (TCS Level2.csv)
#   L2 line:  L2;{side};{yyyyMMddHHmmss};{ffffff};{op};{level};;{price};{size}
#
#   side  : 0 = ask/offer side, 1 = bid side
#           (for L1: trade aggressor side — 0 = at/through ask, 1 = at/through bid)
#   op    : 0 = level inserted/changed, 2 = level dropped (size forced to 0)
#   level : 0-based depth index on that side, 0 = best price
#
# NOTE ON TIMESTAMP PRECISION: Fyers' feed only gives trade time (ltt) to
# 1-second resolution. The sub-second fraction is filled from local
# receipt-time microseconds — it is NOT the exchange's true tick timestamp.

def get_raw_data_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, label, RAW_DATA_FILENAME)


def get_level2_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, label, LEVEL2_FILENAME)


def get_one_second_path(label: str, filename: str = None) -> str:
    return os.path.join(BASE_DATA_DIR, label, filename or ONE_SECOND_FILENAME)


def append_pipe_line(path: str, line: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def write_l1_line(label: str, line: str):
    """Appends one data row to <label>/RawData.csv, writing the header first if new."""
    path = get_raw_data_path(label)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    is_new_file = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", encoding="utf-8") as f:
        if is_new_file:
            f.write(RAW_DATA_HEADER + "\n")
        f.write(line + "\n")


def write_l2_line(label: str, line: str):
    """Appends one 'L2;' or 'L1;' pipe-format line to <label>/Level2.csv."""
    append_pipe_line(get_level2_path(label), line)


def fmt_num(x) -> str:
    """Match C#'s double.ToString(InvariantCulture): no trailing '.0', no sci notation."""
    if x is None:
        return "0"
    try:
        f = float(x)
    except (TypeError, ValueError):
        return str(x)
    if f.is_integer():
        return str(int(f))
    s = f"{f:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"


def fmt_bar_num(x) -> str:
    """Plain float-to-string for 1-second bar files (keeps trailing '.0')."""
    return str(float(x))


def format_nt8_timestamp(dt: datetime, micros_override: int = None):
    """Returns (datePart, fracPart): 'yyyyMMddHHmmss' and 6-digit microseconds."""
    date_part = dt.strftime("%Y%m%d%H%M%S")
    micros = micros_override if micros_override is not None else dt.microsecond
    frac_part = f"{micros:06d}"
    return date_part, frac_part


def format_l1_timestamp(dt: datetime, micros_override: int = None) -> str:
    """Returns 'yyyyMMdd HHmmss fffffff' for RawData.csv (7-digit fraction =
    microseconds x10, to mimic .NET's 100ns tick fraction)."""
    date_time_part = dt.strftime("%Y%m%d %H%M%S")
    micros = micros_override if micros_override is not None else dt.microsecond
    ticks_frac = micros * 10
    frac_part = f"{ticks_frac:07d}"
    return f"{date_time_part} {frac_part}"


def write_one_second_bar(symbol: str, bar_time: datetime, ticks: list):
    """Writes one finalized 1-second OHLCV bar built from actual trade prints.
    Format: timestamp,instrument_key,open,high,low,close,volume"""
    cfg = INSTRUMENTS[symbol]
    path = get_one_second_path(cfg["label"], cfg["one_second_filename"])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    is_new_file = not os.path.exists(path) or os.path.getsize(path) == 0

    prices = [p for p, _ in ticks]
    volumes = [v for _, v in ticks]
    bar_str = bar_time.strftime("%Y-%m-%d %H:%M:%S")

    row = (
        f"{bar_str},{symbol},"
        f"{fmt_bar_num(prices[0])},{fmt_bar_num(max(prices))},"
        f"{fmt_bar_num(min(prices))},{fmt_bar_num(prices[-1])},"
        f"{fmt_bar_num(sum(volumes))}"
    )

    with open(path, "a", encoding="utf-8") as f:
        if is_new_file:
            f.write(ONE_SECOND_HEADER + "\n")
        f.write(row + "\n")


# --- Per-instrument state ---
def _new_state() -> dict:
    return {
        "last_cum_volume": None,
        "last_l1_side": 0,
        "best_bid": None,
        "best_ask": None,
        "last_ask_levels": [],  # list[(price, size)], index 0 = best
        "last_bid_levels": [],
        "logged_unrecognized_depth": False,
        # 1-second OHLCV bucketing
        "one_second_ticks": {},           # epoch_sec -> list[(price, volume)]
        "one_second_last_flushed_epoch": None,
    }


_state = {symbol: _new_state() for symbol in INSTRUMENTS}


def extract_depth_levels(message: dict, max_levels: int):
    """
    Flexible extraction for Fyers' DepthUpdate schema.

    Returns (asks, bids). Each is independently either:
      - a list of (price, size) tuples, best first (possibly empty = side is flat), or
      - None = this message said nothing about that side (leave book state alone).

    Tries nested list-of-dict shape first, then flat numbered keys.
    """
    def conv(levels):
        out = []
        for lvl in levels[:max_levels]:
            if not isinstance(lvl, dict):
                continue
            price = lvl.get("price")
            size = lvl.get("volume", lvl.get("qty", lvl.get("size")))
            if price is None or size is None:
                continue
            try:
                out.append((float(price), float(size)))
            except (TypeError, ValueError):
                continue
        return out

    ask_field = message.get("ask") if isinstance(message.get("ask"), list) else message.get("asks")
    bid_field = message.get("bid") if isinstance(message.get("bid"), list) else message.get("bids")

    asks = conv(ask_field) if isinstance(ask_field, list) else None
    bids = conv(bid_field) if isinstance(bid_field, list) else None

    if asks is not None or bids is not None:
        return asks, bids

    ask_keys_present = any(f"ask_price{i}" in message for i in range(1, max_levels + 1))
    bid_keys_present = any(f"bid_price{i}" in message for i in range(1, max_levels + 1))

    flat_asks, flat_bids = [], []
    for i in range(1, max_levels + 1):
        ap = message.get(f"ask_price{i}")
        asz = message.get(f"ask_size{i}", message.get(f"ask_qty{i}"))
        bp = message.get(f"bid_price{i}")
        bsz = message.get(f"bid_size{i}", message.get(f"bid_qty{i}"))
        if ap is not None and asz is not None:
            try:
                flat_asks.append((float(ap), float(asz)))
            except (TypeError, ValueError):
                pass
        if bp is not None and bsz is not None:
            try:
                flat_bids.append((float(bp), float(bsz)))
            except (TypeError, ValueError):
                pass

    asks = flat_asks if ask_keys_present else None
    bids = flat_bids if bid_keys_present else None
    return asks, bids


def diff_dom_side(side: int, current: list, previous: list, date_part: str, frac_part: str) -> list:
    """Emits a line for every level that changed (op=0) or dropped out (op=2)."""
    out_lines = []
    max_len = max(len(current), len(previous))
    for level in range(max_len):
        has_cur = level < len(current)
        has_prev = level < len(previous)

        if has_cur and (not has_prev or previous[level] != current[level]):
            price, size = current[level]
            out_lines.append(f"L2;{side};{date_part};{frac_part};0;{level};;{fmt_num(price)};{fmt_num(size)}")
        elif not has_cur and has_prev:
            price, _ = previous[level]
            out_lines.append(f"L2;{side};{date_part};{frac_part};2;{level};;{fmt_num(price)};0")

    return out_lines


def handle_trade_message(message: dict):
    """Handles a SymbolUpdate (trade/quote) message for ANY configured symbol.

    -> one row per new trade in <label>/RawData.csv ('Timestamp;Price;Bid;Ask;Volume'),
    -> (if enabled for that symbol) a companion 'L1;side;...' line in Level2.csv,
    -> (if enabled for that symbol) bucketed into the 1-second OHLCV bar.

    Per-trade size is derived from the delta in Fyers' cumulative
    'vol_traded_today'. For futures this is in UNITS (not lots)."""
    symbol = message.get("symbol")
    state = _state.get(symbol)
    cfg = INSTRUMENTS.get(symbol)
    if state is None or cfg is None:
        return
    label = cfg["label"]

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

    prev_cum = state["last_cum_volume"]
    state["last_cum_volume"] = cum_vol
    if prev_cum is None:
        return  # first message just establishes the baseline
    size = cum_vol - prev_cum
    if size <= 0:
        return  # no new trade since the last message

    # The trade message carries the current best bid/ask itself; prefer it.
    msg_bid_price = message.get("bid_price")
    msg_ask_price = message.get("ask_price")

    if msg_ask_price is not None:
        state["best_ask"] = msg_ask_price
    if msg_bid_price is not None:
        state["best_bid"] = msg_bid_price

    best_ask = state["best_ask"]
    best_bid = state["best_bid"]
    if best_ask is not None and ltp >= best_ask:
        side = 0
    elif best_bid is not None and ltp <= best_bid:
        side = 1
    else:
        side = state["last_l1_side"]
    state["last_l1_side"] = side

    ltt = message.get("ltt")
    try:
        trade_dt = datetime.fromtimestamp(int(ltt), tz=IST) if ltt else datetime.now(IST)
    except (TypeError, ValueError, OSError):
        trade_dt = datetime.now(IST)

    # 1-second OHLCV bucket (keyed by the trade's own second)
    if cfg["one_second"]:
        epoch_sec = int(trade_dt.timestamp())
        state["one_second_ticks"].setdefault(epoch_sec, []).append((ltp, size))

    # ltt only has 1-second resolution; use receipt-time microseconds for ordering.
    recv_micros = datetime.now(IST).microsecond
    timestamp_str = format_l1_timestamp(trade_dt, micros_override=recv_micros)
    bid_str = fmt_num(best_bid) if best_bid is not None else fmt_num(ltp)
    ask_str = fmt_num(best_ask) if best_ask is not None else fmt_num(ltp)

    l1_date_part, l1_frac_part = format_nt8_timestamp(trade_dt, micros_override=recv_micros)

    if cfg["split"]:
        lots = max(1, int(round(size)))
        for _ in range(lots):
            line = f"{timestamp_str};{fmt_num(ltp)};{bid_str};{ask_str};1"
            write_l1_line(label, line)
            if cfg["l1_tags"]:
                write_l2_line(label, f"L1;{side};{l1_date_part};{l1_frac_part};{fmt_num(ltp)};1")
    else:
        line = f"{timestamp_str};{fmt_num(ltp)};{bid_str};{ask_str};{fmt_num(size)}"
        write_l1_line(label, line)
        if cfg["l1_tags"]:
            write_l2_line(label, f"L1;{side};{l1_date_part};{l1_frac_part};{fmt_num(ltp)};{fmt_num(size)}")


def handle_depth_message(message: dict):
    """Handles a DepthUpdate message (only for symbols with depth enabled, i.e. TCS).
    Diffs against the previous snapshot and emits 'L2;' lines only for changed
    levels. Each side is handled independently: a side the message says nothing
    about is left untouched."""
    global _raw_depth_debug_count
    if FYERS_DEBUG_RAW_DEPTH_MESSAGES and _raw_depth_debug_count < FYERS_DEBUG_RAW_DEPTH_MESSAGES:
        _raw_depth_debug_count += 1
        logger.info(f"[RAW DEPTH DEBUG {_raw_depth_debug_count}/{FYERS_DEBUG_RAW_DEPTH_MESSAGES}] {message}")

    symbol = message.get("symbol")
    state = _state.get(symbol)
    cfg = INSTRUMENTS.get(symbol)
    if state is None or cfg is None or not cfg["depth"]:
        return  # Level 1-only symbols (Bank Nifty) never write depth
    label = cfg["label"]

    asks, bids = extract_depth_levels(message, DOM_LEVELS)
    if asks is None and bids is None:
        if not state["logged_unrecognized_depth"]:
            state["logged_unrecognized_depth"] = True
            logger.warning(
                f"Unrecognized DepthUpdate schema for {symbol} — no known "
                f"bid/ask fields found. Raw message: {message}"
            )
        return

    now = datetime.now(IST)
    date_part, frac_part = format_nt8_timestamp(now)

    lines = []

    if asks is not None:
        if asks:
            state["best_ask"] = asks[0][0]
        lines += diff_dom_side(0, asks, state["last_ask_levels"], date_part, frac_part)
        state["last_ask_levels"] = asks

    if bids is not None:
        if bids:
            state["best_bid"] = bids[0][0]
        lines += diff_dom_side(1, bids, state["last_bid_levels"], date_part, frac_part)
        state["last_bid_levels"] = bids

    for line in lines:
        write_l2_line(label, line)


# --- Fyers WebSocket callbacks ---
def on_message(message):
    try:
        if not isinstance(message, dict):
            return
        looks_like_depth = any(
            k in message for k in ("ask", "asks", "bid", "bids", "ask_price1", "bid_price1")
        )
        if looks_like_depth:
            handle_depth_message(message)
        elif "ltp" in message:
            handle_trade_message(message)
    except Exception as e:
        logger.error(f"Error parsing feed message: {e}")


def on_error(message):
    logger.error(f"Fyers WebSocket Error: {message}")
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
    # Trades (Level 1) for EVERY configured symbol.
    l1_symbols = list(INSTRUMENTS.keys())
    # Depth (Level 2) only for symbols with depth enabled, and only when the
    # standard feed is the depth source (TBT has its own socket).
    depth_symbols = [s for s, c in INSTRUMENTS.items() if c["depth"]]

    if FYERS_DEPTH_SOURCE == "tbt":
        logger.info(f"Connected to Fyers Data Socket. Subscribing L1 for {l1_symbols} (depth via TBT)...")
        fyers_socket.subscribe(symbols=l1_symbols, data_type="SymbolUpdate")
    else:
        logger.info(
            f"Connected to Fyers Data Socket. Subscribing L1 for {l1_symbols}, "
            f"L2 depth for {depth_symbols}..."
        )
        fyers_socket.subscribe(symbols=l1_symbols, data_type="SymbolUpdate")
        if depth_symbols:
            fyers_socket.subscribe(symbols=depth_symbols, data_type="DepthUpdate")
    fyers_socket.keep_running()


# --- Google Drive helpers ---
def _drive_configured() -> bool:
    return all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN])


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


def upload_file_to_drive(local_path: str, drive_filename: str, folder_name: str):
    """Uploads/updates a single file inside ONE Drive folder named `folder_name`
    at Drive root. REPLACES the Drive copy with local_path's content — safe
    because main() seeds local_path from Drive/GitHub first, so it always
    holds "old + new"."""
    if not os.path.exists(local_path) or not _drive_configured():
        return
    try:
        service = _get_drive_service()
        parent_id = get_or_create_drive_folder(service, folder_name)

        query = f"name = '{drive_filename}' and trashed = false and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)

        if files:
            service.files().update(fileId=files[0]["id"], media_body=media).execute()
        else:
            service.files().create(body={"name": drive_filename, "parents": [parent_id]}, media_body=media).execute()
    except Exception as e:
        logger.error(f"Google Drive Upload Error ({folder_name}/{drive_filename}): {e}")


def download_file_from_drive(local_path: str, drive_filename: str, folder_name: str) -> bool:
    """Seeds local_path with whatever is on Drive for `drive_filename` inside
    `folder_name`, BEFORE any new data is written this run. Returns True only
    when real bytes were restored."""
    if not _drive_configured():
        return False
    try:
        service = _get_drive_service()
        parent_id = get_or_create_drive_folder(service, folder_name)

        query = f"name = '{drive_filename}' and trashed = false and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        if not files:
            logger.info(f"No existing '{drive_filename}' on Drive under '{folder_name}' — starting fresh.")
            return False

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        request = service.files().get_media(fileId=files[0]["id"])
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        logger.info(
            f"Restored '{folder_name}/{drive_filename}' from Drive "
            f"({os.path.getsize(local_path)} bytes) — new data will be appended onto it."
        )
        return os.path.exists(local_path) and os.path.getsize(local_path) > 0
    except Exception as e:
        logger.error(
            f"Google Drive Download Error ({folder_name}/{drive_filename}): {e} — "
            "continuing with a fresh local file for this run."
        )
        return False


# --- GitHub helpers ---
def _github_configured() -> bool:
    return bool(GITHUB_TOKEN and GITHUB_REPOSITORY)


def _github_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def get_github_repo_path(folder_name: str, filename: str) -> str:
    return f"data/{folder_name}/{filename}"


_github_file_sha_cache: dict = {}


def upload_file_to_github(local_path: str, drive_filename: str, folder_name: str):
    """Commits local_path's current content to data/<folder_name>/<drive_filename>
    via the Contents API. Retries on transient connection errors."""
    if not os.path.exists(local_path) or not _github_configured():
        return
    repo_path = get_github_repo_path(folder_name, drive_filename)
    url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPOSITORY}/contents/{repo_path}"

    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("ascii")

    sha = _github_file_sha_cache.get(repo_path)

    for attempt in range(1, GITHUB_MAX_RETRIES + 1):
        try:
            if sha is None:
                get_resp = requests.get(
                    url, headers=_github_headers(), params={"ref": GITHUB_BRANCH},
                    timeout=GITHUB_REQUEST_TIMEOUT_SECONDS,
                )
                if get_resp.status_code == 200:
                    sha = get_resp.json().get("sha")

            payload = {
                "message": f"data: update {repo_path} [skip ci]",
                "content": content_b64,
                "branch": GITHUB_BRANCH,
            }
            if sha:
                payload["sha"] = sha

            put_resp = requests.put(
                url, headers=_github_headers(), json=payload,
                timeout=GITHUB_REQUEST_TIMEOUT_SECONDS,
            )

            if put_resp.status_code in (409, 422):
                sha = None
                _github_file_sha_cache.pop(repo_path, None)
                if attempt < GITHUB_MAX_RETRIES:
                    continue
                logger.error(f"GitHub Upload Error ({repo_path}): {put_resp.status_code} {put_resp.text}")
                return

            if not put_resp.ok:
                logger.error(f"GitHub Upload Error ({repo_path}): {put_resp.status_code} {put_resp.text}")
                return

            new_sha = (put_resp.json().get("content") or {}).get("sha")
            if new_sha:
                _github_file_sha_cache[repo_path] = new_sha
            return  # success

        except requests.exceptions.RequestException as e:
            if attempt < GITHUB_MAX_RETRIES:
                backoff = 3 * attempt
                logger.warning(
                    f"GitHub Upload attempt {attempt}/{GITHUB_MAX_RETRIES} failed "
                    f"({repo_path}): {e} — retrying in {backoff}s."
                )
                sha = None
                time.sleep(backoff)
            else:
                logger.error(
                    f"GitHub Upload Error ({drive_filename}): giving up after "
                    f"{GITHUB_MAX_RETRIES} attempts — {e}"
                )


def download_file_from_github(local_path: str, drive_filename: str, folder_name: str) -> bool:
    """Restores local_path from data/<folder_name>/<drive_filename> in the repo.
    Fallback restore source only.

    NOTE: the Contents API only returns inline base64 'content' for files up to
    ~1 MB, so larger files cannot be restored this way — another reason Bank
    Nifty tick files should rely on Drive rather than GitHub."""
    if not _github_configured():
        return False
    repo_path = get_github_repo_path(folder_name, drive_filename)
    url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPOSITORY}/contents/{repo_path}"

    for attempt in range(1, GITHUB_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url, headers=_github_headers(), params={"ref": GITHUB_BRANCH},
                timeout=GITHUB_REQUEST_TIMEOUT_SECONDS,
            )
            if resp.status_code == 404:
                logger.info(f"No existing '{repo_path}' on GitHub either — starting fresh.")
                return False
            if not resp.ok:
                logger.error(f"GitHub Download Error ({repo_path}): {resp.status_code} {resp.text}")
                return False

            data = resp.json()
            content_b64 = data.get("content", "")
            raw = base64.b64decode(content_b64.encode("ascii")) if content_b64 else b""
            if not raw:
                return False

            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(raw)

            sha = data.get("sha")
            if sha:
                _github_file_sha_cache[repo_path] = sha

            logger.info(
                f"Restored '{repo_path}' from GitHub "
                f"({len(raw)} bytes) — new data will be appended onto it."
            )
            return True

        except requests.exceptions.RequestException as e:
            if attempt < GITHUB_MAX_RETRIES:
                backoff = 3 * attempt
                logger.warning(
                    f"GitHub Download attempt {attempt}/{GITHUB_MAX_RETRIES} failed "
                    f"({repo_path}): {e} — retrying in {backoff}s."
                )
                time.sleep(backoff)
            else:
                logger.error(
                    f"GitHub Download Error ({drive_filename}): giving up after "
                    f"{GITHUB_MAX_RETRIES} attempts — {e} — continuing without a GitHub restore."
                )
                return False
    return False


def build_sync_files() -> list:
    """Every running file to back up: (label, filename, github_key, drive_interval_seconds)."""
    files = [
        (TCS_LABEL, RAW_DATA_FILENAME, "raw", DRIVE_SYNC_INTERVAL_SECONDS),
        (TCS_LABEL, LEVEL2_FILENAME, "level2", DRIVE_SYNC_INTERVAL_SECONDS),
        (TCS_LABEL, ONE_SECOND_FILENAME, "1s", DRIVE_SYNC_INTERVAL_SECONDS),
    ]
    if BN_ENABLED:
        files.append((BN_LABEL, RAW_DATA_FILENAME, "bn_raw", BN_DRIVE_SYNC_INTERVAL_SECONDS))
        if BN_ONE_SECOND:
            files.append((BN_LABEL, BN_ONE_SECOND_FILENAME, "bn_1s", BN_DRIVE_SYNC_INTERVAL_SECONDS))
    return files


async def cloud_sync_loop():
    """Backs up every running file to Google Drive (each on its own cadence,
    skipping files that haven't grown since their last upload) and, as an
    independent second backup, to GitHub (throttled; only files named in
    GITHUB_SYNC_FILES).

    Drive uploads are spaced a second apart to reduce the odds of tripping
    Drive's userRateLimitExceeded burst limit."""
    sync_files = build_sync_files()
    last_drive_sync = {}      # (label, filename) -> time
    last_drive_size = {}      # (label, filename) -> bytes at last upload
    last_github_sync = 0.0

    while True:
        await asyncio.sleep(10)

        for label, filename, _gh_key, interval in sync_files:
            key = (label, filename)
            path = os.path.join(BASE_DATA_DIR, label, filename)
            if not os.path.exists(path):
                continue
            if (time.time() - last_drive_sync.get(key, 0.0)) < interval:
                continue
            size = os.path.getsize(path)
            if size == last_drive_size.get(key):
                continue  # nothing new since last upload
            last_drive_sync[key] = time.time()
            last_drive_size[key] = size
            await asyncio.to_thread(upload_file_to_drive, path, filename, label)
            await asyncio.sleep(1)

        if _github_configured() and (time.time() - last_github_sync) >= GITHUB_SYNC_INTERVAL_SECONDS:
            last_github_sync = time.time()
            for label, filename, gh_key, _interval in sync_files:
                if gh_key in GITHUB_SYNC_FILES:
                    path = os.path.join(BASE_DATA_DIR, label, filename)
                    await asyncio.to_thread(upload_file_to_github, path, filename, label)


# =========================================================
# --- 1-second OHLCV bar aggregation ---
# =========================================================
# Trade prints are bucketed by their own trade-second into state["one_second_ticks"];
# this loop finalizes/writes any bucket old enough (ONE_SECOND_BUFFER_SECONDS) that
# no more late trades are expected. Seconds with no trades are SKIPPED.
# Runs for every symbol with one_second enabled (TCS always; Bank Nifty only if
# FYERS_BANKNIFTY_1S=true). Depth/Level2 data is never touched here.

def flush_ready_one_second_bars():
    now_epoch = int(time.time())
    cutoff = now_epoch - ONE_SECOND_BUFFER_SECONDS

    for symbol, cfg in INSTRUMENTS.items():
        if not cfg["one_second"]:
            continue
        state = _state[symbol]

        if state["one_second_last_flushed_epoch"] is None:
            state["one_second_last_flushed_epoch"] = cutoff - 1
            continue

        for epoch_sec in range(state["one_second_last_flushed_epoch"] + 1, cutoff + 1):
            ticks = state["one_second_ticks"].pop(epoch_sec, None)
            if not ticks:
                continue
            bar_time = datetime.fromtimestamp(epoch_sec, tz=IST)
            write_one_second_bar(symbol, bar_time, ticks)

        state["one_second_last_flushed_epoch"] = cutoff


async def one_second_timer_loop():
    """Wakes roughly once a second to finalize 1-second bars outside the buffer window."""
    while True:
        now = time.time()
        await asyncio.sleep(1.0 - (now % 1.0))
        try:
            flush_ready_one_second_bars()
        except Exception as e:
            logger.error(f"1-second bar flush error: {e}")


# =========================================================
# --- TBT (Versova) 50-level depth feed — opt-in (TCS only) ---
# =========================================================
# TBT sends a FULL snapshot on the first message per symbol, then INCREMENTAL
# per-level updates, so a persistent per-level book is kept and only levels
# present in each message are compared/emitted.
_tbt_book = {
    "asks": {i: None for i in range(DOM_LEVELS)},
    "bids": {i: None for i in range(DOM_LEVELS)},
}


def _tbt_apply_side(side_label: str, levels, side_code: int, date_part: str, frac_part: str) -> list:
    """Merges incoming MarketLevel entries into the persistent book for one
    side, emitting an L2 line only for levels whose value changed. price==0
    with qty>0 is a known TBT anomaly — keep previous price, update qty."""
    book = _tbt_book[side_label]
    out_lines = []
    for lvl in levels:
        level = lvl.num.value
        if level >= DOM_LEVELS:
            continue
        price = lvl.price.value / 100.0
        qty = float(lvl.qty.value)
        prev = book.get(level)

        if price == 0.0 and qty > 0 and prev is not None:
            price = prev[0]

        new_val = (price, qty)
        if new_val == prev:
            continue
        book[level] = new_val

        if qty == 0:
            out_lines.append(f"L2;{side_code};{date_part};{frac_part};2;{level};;{fmt_num(price)};0")
        else:
            out_lines.append(f"L2;{side_code};{date_part};{frac_part};0;{level};;{fmt_num(price)};{fmt_num(qty)}")
    return out_lines


def handle_tbt_socket_message(data: bytes):
    """Parses one binary TBT SocketMessage and emits 'L2;' lines (TCS Level2.csv)."""
    socket_message = fyers_tbt_pb2.SocketMessage()
    socket_message.ParseFromString(data)

    if socket_message.error:
        logger.error(f"TBT socket error: {socket_message.msg}")
        return

    for ticker, feed in socket_message.feeds.items():
        if not feed.HasField("depth"):
            continue

        if feed.snapshot:
            _tbt_book["asks"] = {i: None for i in range(DOM_LEVELS)}
            _tbt_book["bids"] = {i: None for i in range(DOM_LEVELS)}

        now = datetime.now(IST)
        date_part, frac_part = format_nt8_timestamp(now)

        lines = _tbt_apply_side("asks", feed.depth.asks, 0, date_part, frac_part)
        lines += _tbt_apply_side("bids", feed.depth.bids, 1, date_part, frac_part)

        for line in lines:
            write_l2_line(TCS_LABEL, line)


async def run_tbt_depth_with_retry():
    """Connects to Fyers' TBT/Versova WebSocket. Only runs when FYERS_DEPTH_SOURCE=tbt."""
    if fyers_tbt_pb2 is None:
        raise RuntimeError(
            "FYERS_DEPTH_SOURCE=tbt requires msg_pb2.py (compiled from the "
            "Fyers TBT msg.proto) to be present next to this script, and the "
            "'websockets' and 'protobuf' packages installed."
        )

    backoff = 5
    while True:
        try:
            access_token = get_valid_access_token()
            auth_header = f"{FYERS_APP_ID}-{FYERS_APP_TYPE}:{access_token}"

            logger.info(f"Connecting to Fyers TBT depth feed at {FYERS_TBT_WS_URL} for {TCS_SYMBOL}...")
            async with websockets.connect(
                FYERS_TBT_WS_URL,
                additional_headers={"Authorization": auth_header},
                max_size=None,
            ) as ws:
                subscribe_msg = {
                    "type": 1,
                    "data": {
                        "subs": 1,
                        "symbols": [TCS_SYMBOL],
                        "mode": "depth",
                        "channel": "1",
                    },
                }
                await ws.send(json.dumps(subscribe_msg))
                await ws.send(json.dumps({
                    "type": 2,
                    "data": {"resumeChannels": ["1"], "pauseChannels": []},
                }))
                logger.info("TBT depth feed subscribed.")
                backoff = 5

                last_ping = time.time()
                while True:
                    if time.time() - last_ping >= 30:
                        await ws.send("ping")
                        last_ping = time.time()
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue
                    if isinstance(message, bytes):
                        try:
                            handle_tbt_socket_message(message)
                        except Exception as e:
                            logger.error(f"Error parsing TBT message: {e}")
                    else:
                        logger.info(f"TBT text message: {message}")
        except Exception as e:
            logger.error(f"TBT depth socket crashed: {e} — retrying in {backoff}s. "
                         "If this keeps happening, TBT may not be enabled for this "
                         "symbol/segment on your account (it's currently NFO-only "
                         "per Fyers' docs) — check whether NSE:TCS-EQ is supported.")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def run_websocket_with_retry():
    """Keep the data socket alive, regenerating the token if the server rejects
    it and reconnecting with backoff instead of dying silently."""
    backoff = 5
    consecutive_failures = 0
    max_consecutive_failures = 6
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
            backoff = 5
            consecutive_failures = 0
            while True:
                await asyncio.sleep(30)
        except Exception as e:
            consecutive_failures += 1
            logger.error(f"WebSocket loop crashed: {e} — retrying in {backoff}s")
            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    f"Giving up after {consecutive_failures} consecutive failures — "
                    "this looks like a credentials/config problem, not a transient outage. "
                    "Check FYERS_FY_ID / FYERS_TOTP_SECRET / FYERS_PIN / FYERS_APP_ID / "
                    "FYERS_SECRET_KEY / FYERS_REDIRECT_URI secrets."
                )
                raise
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def main():
    os.makedirs(BASE_DATA_DIR, exist_ok=True)

    logger.info(
        f"Starting real-time collection — TCS ({TCS_SYMBOL}): L1+L2+1s "
        f"(depth source: {FYERS_DEPTH_SOURCE})"
        + (f" | Bank Nifty futures ({BN_SYMBOL}): L1 raw ticks"
           f"{' + 1s bars' if BN_ONE_SECOND else ''}" if BN_ENABLED else " | Bank Nifty disabled")
    )

    # Restore prior data BEFORE anything writes locally, so the first cloud
    # sync appends onto history instead of overwriting it with a near-empty
    # file. Drive first; GitHub as fallback. Only restores when the local
    # file is missing/empty.
    async def restore_running_file(label: str, filename: str):
        local_path = os.path.join(BASE_DATA_DIR, label, filename)
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            return
        restored = False
        if _drive_configured():
            restored = await asyncio.to_thread(download_file_from_drive, local_path, filename, label)
        if not restored and _github_configured():
            logger.info(f"Drive didn't restore '{label}/{filename}' — falling back to GitHub.")
            await asyncio.to_thread(download_file_from_github, local_path, filename, label)

    for label, filename, _gh_key, _interval in build_sync_files():
        await restore_running_file(label, filename)

    tasks = [run_websocket_with_retry(), cloud_sync_loop(), one_second_timer_loop()]
    if FYERS_DEPTH_SOURCE == "tbt":
        tasks.append(run_tbt_depth_with_retry())

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Collector stopped manually.")
