import os
import sys
import time
import json
import io
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

# TBT (Versova) protobuf schema — generated from Fyers' official msg.proto,
# only needed when FYERS_DEPTH_SOURCE=tbt. Ship msg_pb2.py alongside this
# file (see accompanying msg.proto / build instructions).
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

# --- Fyers app credentials (from your Fyers API app) ---
def _normalize_app_id(raw: str, app_type: str) -> str:
    """
    Accepts either the bare app id ('XY0W1234') or the full client-id
    format ('XY0W1234-100') for FYERS_APP_ID and always returns the bare
    app id — the '-<app_type>' suffix is appended back on wherever a full
    client_id is actually needed. Makes the script work regardless of
    which format was pasted into the secret.
    """
    if not raw:
        return raw
    raw = raw.strip()
    suffix = f"-{app_type}"
    if raw.endswith(suffix):
        return raw[: -len(suffix)]
    # Also handle a stray "-<anything>" suffix in case FYERS_APP_TYPE
    # itself doesn't match what's embedded in the id.
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

# --- Instrument (single symbol, Level 1 + Level 2) ---
FYERS_SYMBOL = os.environ.get("FYERS_SYMBOL", "NSE:TCS-EQ")
INSTRUMENT_LABEL = "tcs"
# NOTE ON DEPTH: how many levels/side you actually receive from Fyers'
# "DepthUpdate" feed depends on the instrument/segment and your Fyers plan
# -- some feeds send back more than the old 5-level assumption. This script
# just requests/accepts up to DOM_LEVELS per side; extract_depth_levels()
# slices to that cap, so if Fyers sends fewer levels for a given symbol you
# simply get fewer, and if it sends more you now capture up to 50.
DOM_LEVELS = int(os.environ.get("FYERS_DOM_LEVELS", "50"))

# --- Depth source selection ---
# "standard" (default): the existing data_ws DepthUpdate feed used below.
# For equity cash symbols (like NSE:TCS-EQ) Fyers only ever sends a handful
# of levels (5, occasionally a bit more) on this feed regardless of what
# DOM_LEVELS is set to — there is no server-side way to ask for more.
#
# "tbt": Fyers' separate Tick-By-Tick / "Versova" protobuf WebSocket, which
# is the ONLY Fyers feed that actually carries up to 50 depth levels.
# IMPORTANT CAVEAT (confirmed from Fyers' own docs/community as of writing):
# TBT is currently available for NFO (NSE F&O) instruments only — NOT for
# NSE cash-market equities such as NSE:TCS-EQ. If you point this at an
# equity symbol, expect either no data or a connection/auth rejection.
# If you need 50-level depth on TCS specifically, you'd need to trade the
# TCS futures contract's symbol instead, or confirm with Fyers support
# whether NSECM TBT has since been enabled for your account.
FYERS_DEPTH_SOURCE = os.environ.get("FYERS_DEPTH_SOURCE", "standard").strip().lower()
FYERS_TBT_WS_URL = os.environ.get("FYERS_TBT_WS_URL", "wss://rtsocket-api.fyers.in/versova")

# Temporary debug aid: log the first N raw DepthUpdate messages, completely
# unparsed, so you can see exactly what fields/levels Fyers is actually
# sending (rather than trusting what extract_depth_levels() picked out).
# Set to 0 to disable. Safe to leave on briefly, then turn back off --
# it only fires a bounded number of times, not on every message.
FYERS_DEBUG_RAW_DEPTH_MESSAGES = int(os.environ.get("FYERS_DEBUG_RAW_DEPTH_MESSAGES", "5"))
_raw_depth_debug_count = 0

# Optional: split each trade print into N separate 1-volume lines, matching
# the C# indicator's "Split multi-lot prints into 1-lot lines (matches NT8
# 1-Volume series)" behavior. On by default, like the C# script.
FYERS_SPLIT_PRINTS = os.environ.get("FYERS_SPLIT_PRINTS", "true").strip().lower() not in ("0", "false", "no")

BASE_DATA_DIR = "data"

# Local + Drive layout: ONE folder per instrument (e.g. "tcs"), containing
# exactly two running files — RawData.csv (L1 tape) and Level2.csv (L2 DOM
# diffs). No daily sub-folders, no duplicate daily/combined pairs.
RAW_DATA_FILENAME = "RawData.csv"
LEVEL2_FILENAME = "Level2.csv"

# Header row written once at the top of a brand-new RawData.csv.
RAW_DATA_HEADER = "Timestamp;Price;Bid;Ask;Volume"


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

    # Step 1: send login OTP request for the client id
    r1 = _post(f"{base}/send_login_otp", {"fy_id": FYERS_FY_ID, "app_id": "2"}, "send_login_otp")
    request_key = r1.json()["request_key"]

    # Step 2: verify TOTP
    totp_code = pyotp.TOTP(FYERS_TOTP_SECRET).now()
    r2 = _post(f"{base}/verify_otp", {"request_key": request_key, "otp": totp_code}, "verify_otp")
    request_key_2 = r2.json()["request_key"]

    # Step 3: verify PIN, get an internal session token
    r3 = _post(f"{base}/verify_pin", {
        "request_key": request_key_2,
        "identity_type": "pin",
        "identifier": FYERS_PIN
    }, "verify_pin")
    internal_token = r3.json()["data"]["access_token"]

    # Step 4: exchange internal session token for an auth_code against your app
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
    # IMPORTANT: this endpoint returns HTTP 308 on success with the
    # redirect URL in the JSON body — it must NOT be auto-followed,
    # or requests silently POSTs to redirect_uri instead and you get
    # an unrelated 400 back from that page.
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
# --- L1/L2 output (RawData.csv: Timestamp;Price;Bid;Ask;Volume)
# --- (Level2.csv unchanged: matches UltraLinkQuantowerBridge_GDrive.cs) ---
# =========================================================
#
#   L1 line:  {yyyyMMdd HHmmss fffffff};{price};{bid};{ask};{volume}
#   L2 line:  L2;{side};{yyyyMMddHHmmss};{ffffff};{op};{level};;{price};{size}
#
#   side  : 0 = ask/offer side, 1 = bid side
#   op    : 0 = level inserted/changed, 2 = level dropped (size forced to 0)
#   level : 0-based depth index on that side, 0 = best price
#
# RawData.csv gets one header row ("Timestamp;Price;Bid;Ask;Volume") the
# first time the file is created, then one data row per trade: the trade
# timestamp, the trade price, and the best bid/ask in force *at that
# moment*, plus the trade's volume.
#
# NOTE ON TIMESTAMP PRECISION: Fyers' feed only gives trade time (ltt) to
# 1-second resolution and doesn't expose exchange-side sub-second ticks the
# way the raw NT8 tape does. The 7-digit fraction here is filled from local
# receipt-time microseconds (scaled x10 to approximate .NET's 100ns "tick"
# fraction) so lines stay strictly orderable and match the expected 7-digit
# width — it is NOT the exchange's true tick timestamp.

def get_raw_data_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, label, RAW_DATA_FILENAME)


def get_level2_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, label, LEVEL2_FILENAME)


def append_pipe_line(path: str, line: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def write_l1_line(label: str, line: str):
    """Appends one data row to <label>/RawData.csv, writing the
    'Timestamp;Price;Bid;Ask;Volume' header first if the file is new."""
    path = get_raw_data_path(label)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    is_new_file = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", encoding="utf-8") as f:
        if is_new_file:
            f.write(RAW_DATA_HEADER + "\n")
        f.write(line + "\n")


def write_l2_line(label: str, line: str):
    """Appends one 'L2;' DOM-diff line to <label>/Level2.csv."""
    append_pipe_line(get_level2_path(label), line)


def fmt_num(x) -> str:
    """Match C#'s double.ToString(InvariantCulture): plain decimal, no
    trailing '.0' for whole numbers, no scientific notation."""
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


def format_nt8_timestamp(dt: datetime, micros_override: int = None):
    """Returns (datePart, fracPart) as 'yyyyMMddHHmmss' and 6-digit
    microseconds, matching FormatNt8Timestamp() in the C# source. Used for
    the L2 (Level2.csv) lines only."""
    date_part = dt.strftime("%Y%m%d%H%M%S")
    micros = micros_override if micros_override is not None else dt.microsecond
    frac_part = f"{micros:06d}"
    return date_part, frac_part


def format_l1_timestamp(dt: datetime, micros_override: int = None) -> str:
    """Returns a single 'yyyyMMdd HHmmss fffffff' string for RawData.csv —
    a space-separated date, time, and 7-digit sub-second fraction. Python
    only has microsecond (6-digit) resolution, so the fraction is scaled
    x10 to fill the 7-digit width the same way .NET's 100ns tick fraction
    would (e.g. .390680s -> 3906800)."""
    date_time_part = dt.strftime("%Y%m%d %H%M%S")
    micros = micros_override if micros_override is not None else dt.microsecond
    ticks_frac = micros * 10
    frac_part = f"{ticks_frac:07d}"
    return f"{date_time_part} {frac_part}"


# --- Per-instrument state (single symbol here, but keyed for extensibility) ---
_state = {
    FYERS_SYMBOL: {
        "last_cum_volume": None,
        "last_l1_side": 0,
        "best_bid": None,
        "best_ask": None,
        "last_ask_levels": [],  # list[(price, size)], index 0 = best
        "last_bid_levels": [],
        "logged_unrecognized_depth": False,
    }
}


def extract_depth_levels(message: dict, max_levels: int):
    """
    Flexible extraction to cope with Fyers' undocumented DepthUpdate schema.
    Tries, in order:
      1. Nested list-of-dict shape confirmed from Fyers' REST depth() API:
         message['ask' or 'asks'] / message['bid' or 'bids'], each a list of
         {'price': ..., 'volume': ...} (or 'qty'/'size').
      2. Flat numbered keys: ask_price{i}/ask_size{i}(or ask_qty{i}),
         bid_price{i}/bid_size{i}(or bid_qty{i}) for i in 1..max_levels.
    Returns (asks, bids) as lists of (price, size) tuples, best price first,
    or (None, None) if neither shape matches.
    """
    ask_raw = message.get("ask") if isinstance(message.get("ask"), list) else message.get("asks")
    bid_raw = message.get("bid") if isinstance(message.get("bid"), list) else message.get("bids")

    if isinstance(ask_raw, list) and isinstance(bid_raw, list) and (ask_raw or bid_raw):
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
        return conv(ask_raw), conv(bid_raw)

    asks, bids = [], []
    for i in range(1, max_levels + 1):
        ap = message.get(f"ask_price{i}")
        asz = message.get(f"ask_size{i}", message.get(f"ask_qty{i}"))
        bp = message.get(f"bid_price{i}")
        bsz = message.get(f"bid_size{i}", message.get(f"bid_qty{i}"))
        if ap is not None and asz is not None:
            try:
                asks.append((float(ap), float(asz)))
            except (TypeError, ValueError):
                pass
        if bp is not None and bsz is not None:
            try:
                bids.append((float(bp), float(bsz)))
            except (TypeError, ValueError):
                pass

    if asks or bids:
        return asks, bids

    return None, None


def diff_dom_side(side: int, current: list, previous: list, date_part: str, frac_part: str) -> list:
    """Mirrors DiffDomSide() in the C# source: emits a line for every level
    that changed price/size (op=0) or dropped out of the book (op=2, size
    forced to 0). Only emits for levels that actually changed."""
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
    """Handles a SymbolUpdate (trade/quote) message -> emits one row per new
    trade to RawData.csv in 'Timestamp;Price;Bid;Ask;Volume' format. Per-trade
    size is derived from the delta in Fyers' cumulative 'vol_traded_today'
    field (Fyers doesn't reliably expose a per-tick 'ltq' on this feed in all
    cases)."""
    symbol = message.get("symbol")
    state = _state.get(symbol)
    if state is None:
        return

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

    # ltt only has 1-second resolution; use current receipt-time microseconds
    # so lines stay orderable (see module-level NOTE above).
    recv_micros = datetime.now(IST).microsecond
    timestamp_str = format_l1_timestamp(trade_dt, micros_override=recv_micros)
    # Matches the C# source: if no bid/ask quote has arrived yet, fall back
    # to the trade price itself rather than writing "0".
    bid_str = fmt_num(best_bid) if best_bid is not None else fmt_num(ltp)
    ask_str = fmt_num(best_ask) if best_ask is not None else fmt_num(ltp)

    if FYERS_SPLIT_PRINTS:
        # Matches the C# script's SplitPrints: emit `lots` separate rows,
        # each with volume=1, all sharing the same timestamp/price/bid/ask
        # (same trade moment) -- so a 5-lot print becomes 5 rows instead of 1.
        lots = max(1, int(round(size)))
        for _ in range(lots):
            line = f"{timestamp_str};{fmt_num(ltp)};{bid_str};{ask_str};1"
            write_l1_line(INSTRUMENT_LABEL, line)
    else:
        line = f"{timestamp_str};{fmt_num(ltp)};{bid_str};{ask_str};{fmt_num(size)}"
        write_l1_line(INSTRUMENT_LABEL, line)


def handle_depth_message(message: dict):
    """Handles a DepthUpdate message -> diffs against the previous snapshot
    and emits 'L2;' lines only for levels that actually changed, written to
    the L2-only files."""
    global _raw_depth_debug_count
    if FYERS_DEBUG_RAW_DEPTH_MESSAGES and _raw_depth_debug_count < FYERS_DEBUG_RAW_DEPTH_MESSAGES:
        _raw_depth_debug_count += 1
        logger.info(f"[RAW DEPTH DEBUG {_raw_depth_debug_count}/{FYERS_DEBUG_RAW_DEPTH_MESSAGES}] {message}")

    symbol = message.get("symbol")
    state = _state.get(symbol)
    if state is None:
        return

    asks, bids = extract_depth_levels(message, DOM_LEVELS)
    if asks is None and bids is None:
        if not state["logged_unrecognized_depth"]:
            state["logged_unrecognized_depth"] = True
            logger.warning(
                f"Unrecognized DepthUpdate schema for {symbol} — no known "
                f"bid/ask fields found. Raw message: {message}"
            )
        return

    asks = asks or []
    bids = bids or []
    if asks:
        state["best_ask"] = asks[0][0]
    if bids:
        state["best_bid"] = bids[0][0]

    now = datetime.now(IST)
    date_part, frac_part = format_nt8_timestamp(now)

    lines = diff_dom_side(0, asks, state["last_ask_levels"], date_part, frac_part)
    lines += diff_dom_side(1, bids, state["last_bid_levels"], date_part, frac_part)

    state["last_ask_levels"] = asks
    state["last_bid_levels"] = bids

    for line in lines:
        write_l2_line(INSTRUMENT_LABEL, line)


# --- Fyers WebSocket callbacks ---
def on_message(message):
    try:
        if not isinstance(message, dict):
            return
        # Route by shape rather than relying on an undocumented "type" key:
        # a depth-ish message carries book fields; a trade/quote message
        # carries 'ltp'. Depth is checked first since some feeds echo ltp
        # on depth snapshots too.
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
    if FYERS_DEPTH_SOURCE == "tbt":
        # Depth is coming from the separate TBT socket in this mode — only
        # take trades (L1) from the standard socket, to avoid writing
        # Level2.csv from two independent feeds at once.
        logger.info(f"Connected to Fyers Data Socket. Subscribing to {FYERS_SYMBOL} (L1 only — depth via TBT)...")
        fyers_socket.subscribe(symbols=[FYERS_SYMBOL], data_type="SymbolUpdate")
    else:
        logger.info(f"Connected to Fyers Data Socket. Subscribing to {FYERS_SYMBOL} (L1 + L2)...")
        fyers_socket.subscribe(symbols=[FYERS_SYMBOL], data_type="SymbolUpdate")
        fyers_socket.subscribe(symbols=[FYERS_SYMBOL], data_type="DepthUpdate")
    fyers_socket.keep_running()


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


def upload_file_to_drive(local_path: str, drive_filename: str, folder_name: str):
    """Uploads/updates a single file inside ONE Drive folder named
    `folder_name` (e.g. "tcs") sitting at Drive root — no date subfolders.

    This REPLACES whatever is currently on Drive with local_path's content.
    That's only safe because download_file_from_drive() (called once at
    startup, see main()) seeds local_path with the prior Drive content
    first — so local_path always holds "old + new", never just "new"."""
    if not os.path.exists(local_path) or not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
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
        logger.error(f"Google Drive Upload Error ({drive_filename}): {e}")


def download_file_from_drive(local_path: str, drive_filename: str, folder_name: str):
    """Seeds local_path with whatever is currently on Drive for
    `drive_filename` inside `folder_name`, BEFORE any new data is written
    this run.

    Why this exists: this job typically restarts on an ephemeral filesystem
    (fresh container), so local_path starts empty every run. Without this,
    the first google_drive_sync_loop() cycle would call upload_file_to_drive(),
    which does an in-place Drive *replace* — overwriting all of yesterday's
    (or this morning's) accumulated history with just the handful of rows
    written since restart. Downloading first means local_path already
    contains the full prior history, so every subsequent write appends onto
    it and every subsequent Drive sync re-uploads "old + new" rather than
    "new only" — i.e. data combines across restarts instead of being wiped.

    No-ops (leaves local_path alone) if Drive isn't configured, if nothing
    exists there yet (first-ever run), or on any error — so a transient
    Drive hiccup at startup degrades to "start fresh locally" rather than
    crashing the job.
    """
    if not all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
        return
    try:
        service = _get_drive_service()
        parent_id = get_or_create_drive_folder(service, folder_name)

        query = f"name = '{drive_filename}' and trashed = false and '{parent_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        if not files:
            logger.info(f"No existing '{drive_filename}' on Drive under '{folder_name}' — starting fresh.")
            return

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        request = service.files().get_media(fileId=files[0]["id"])
        with io.FileIO(local_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        logger.info(
            f"Restored '{drive_filename}' from Drive "
            f"({os.path.getsize(local_path)} bytes) — new data will be appended onto it."
        )
    except Exception as e:
        logger.error(
            f"Google Drive Download Error ({drive_filename}): {e} — "
            "continuing with a fresh local file for this run."
        )


async def google_drive_sync_loop():
    """Syncs exactly two files per cycle, both inside the same Drive folder
    named after the instrument (e.g. "tcs/RawData.csv" and
    "tcs/Level2.csv")."""
    while True:
        await asyncio.sleep(10)

        raw_path = get_raw_data_path(INSTRUMENT_LABEL)
        await asyncio.to_thread(upload_file_to_drive, raw_path, RAW_DATA_FILENAME, INSTRUMENT_LABEL)

        level2_path = get_level2_path(INSTRUMENT_LABEL)
        await asyncio.to_thread(upload_file_to_drive, level2_path, LEVEL2_FILENAME, INSTRUMENT_LABEL)


# =========================================================
# --- TBT (Versova) 50-level depth feed — opt-in, protobuf over WS ---
# =========================================================
# IMPORTANT: unlike the standard DepthUpdate feed (which appears to send a
# full book snapshot on every message, hence the simple diff_dom_side()
# full-list-vs-full-list comparison used above), Fyers' TBT feed sends a
# FULL snapshot only on the first message per symbol (feed.snapshot=True),
# and INCREMENTAL per-level updates after that — a message may carry only
# one changed level. So this keeps a persistent 50-slot book per side and
# only compares/emits for the levels actually present in each message;
# levels not mentioned in an incremental update are left untouched (NOT
# treated as dropped — that was wrong in an earlier draft of this code).
_tbt_book = {
    "asks": {i: None for i in range(DOM_LEVELS)},  # level -> (price, qty) or None
    "bids": {i: None for i in range(DOM_LEVELS)},
}


def _tbt_apply_side(side_label: str, levels, side_code: int, date_part: str, frac_part: str) -> list:
    """Merges incoming MarketLevel entries into the persistent per-level
    book for one side, emitting an L2 line only for levels that actually
    changed value. price==0 with qty>0 is a known TBT anomaly (per Fyers'
    own reference app) — treated as 'keep previous price, update qty'."""
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
            price = prev[0]  # anomaly: preserve last known price

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
    """Parses one binary TBT SocketMessage and emits 'L2;' lines for
    whichever levels actually changed, in the same wire format as the
    standard-feed path so Level2.csv stays consistent."""
    socket_message = fyers_tbt_pb2.SocketMessage()
    socket_message.ParseFromString(data)

    if socket_message.error:
        logger.error(f"TBT socket error: {socket_message.msg}")
        return

    for ticker, feed in socket_message.feeds.items():
        if not feed.HasField("depth"):
            continue

        if feed.snapshot:
            # Full snapshot: reset the book so stale levels from a previous
            # session don't linger, then apply as normal.
            _tbt_book["asks"] = {i: None for i in range(DOM_LEVELS)}
            _tbt_book["bids"] = {i: None for i in range(DOM_LEVELS)}

        now = datetime.now(IST)
        date_part, frac_part = format_nt8_timestamp(now)

        lines = _tbt_apply_side("asks", feed.depth.asks, 0, date_part, frac_part)
        lines += _tbt_apply_side("bids", feed.depth.bids, 1, date_part, frac_part)

        for line in lines:
            write_l2_line(INSTRUMENT_LABEL, line)


async def run_tbt_depth_with_retry():
    """Connects to Fyers' TBT/Versova WebSocket for real 50-level depth.
    Only runs when FYERS_DEPTH_SOURCE=tbt. Reconnects with backoff, same
    pattern as run_websocket_with_retry()."""
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

            logger.info(f"Connecting to Fyers TBT depth feed at {FYERS_TBT_WS_URL} for {FYERS_SYMBOL}...")
            async with websockets.connect(
                FYERS_TBT_WS_URL,
                additional_headers={"Authorization": auth_header},
                max_size=None,
            ) as ws:
                subscribe_msg = {
                    "type": 1,
                    "data": {
                        "subs": 1,
                        "symbols": [FYERS_SYMBOL],
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
    """Keep the data socket alive, regenerating the token if the server
    rejects it and reconnecting with backoff instead of dying silently."""
    backoff = 5
    consecutive_failures = 0
    max_consecutive_failures = 6  # stop wasting the 6h job on bad credentials
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
            consecutive_failures = 0
            # The SDK's own thread drives the socket; just idle here and
            # let google_drive_sync_loop keep running.
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

    logger.info(f"Starting real-time L1+L2 collection for {FYERS_SYMBOL} "
                f"(depth source: {FYERS_DEPTH_SOURCE})...")

    # Restore prior data from Drive BEFORE anything writes locally. This is
    # the fix for the "overwrites previous data" problem: if this run's
    # local RawData.csv/Level2.csv start empty (fresh container) and we
    # skip this step, the first google_drive_sync_loop() upload replaces
    # Drive's accumulated file with an almost-empty one. Pulling the
    # existing Drive copy down first means local writes append onto the
    # full history, so it keeps combining across restarts instead of
    # resetting. Only restores when the local file is missing/empty, so it
    # never clobbers a local file that already has this run's data (e.g. on
    # a non-ephemeral host where local state survived a restart).
    raw_path = get_raw_data_path(INSTRUMENT_LABEL)
    level2_path = get_level2_path(INSTRUMENT_LABEL)
    if not os.path.exists(raw_path) or os.path.getsize(raw_path) == 0:
        await asyncio.to_thread(download_file_from_drive, raw_path, RAW_DATA_FILENAME, INSTRUMENT_LABEL)
    if not os.path.exists(level2_path) or os.path.getsize(level2_path) == 0:
        await asyncio.to_thread(download_file_from_drive, level2_path, LEVEL2_FILENAME, INSTRUMENT_LABEL)

    tasks = [run_websocket_with_retry(), google_drive_sync_loop()]
    if FYERS_DEPTH_SOURCE == "tbt":
        tasks.append(run_tbt_depth_with_retry())

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Collector stopped manually.")
