import os
import sys
import time
import json
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

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
# --- L1/L2 pipe-format output (matches UltraLinkQuantowerBridge_GDrive.cs) ---
# =========================================================
#
#   L1 line:  L1;{side};{yyyyMMddHHmmss};{ffffff};{price};{size}
#   L2 line:  L2;{side};{yyyyMMddHHmmss};{ffffff};{op};{level};;{price};{size}
#
#   side  : 0 = ask/offer side, 1 = bid side
#   op    : 0 = level inserted/changed, 2 = level dropped (size forced to 0)
#   level : 0-based depth index on that side, 0 = best price
#
# CHANGED FROM THE C# SOURCE: the C# indicator interleaves L1 and L2 lines
# into ONE local file (see its header comment). This collector instead
# writes them to TWO SEPARATE, ever-growing files — one for the L1 tape,
# one for the L2 DOM diffs — both living inside a single per-instrument
# folder (e.g. "tcs/"), while keeping the exact same per-line wire format
# for each line type. No daily rotation, no duplicate daily+combined
# copies — just RawData.csv and Level2.csv, appended to forever.
#
# NOTE ON TIMESTAMP PRECISION: Fyers' feed only gives trade time (ltt) to
# 1-second resolution and doesn't expose exchange-side microseconds the way
# the raw NT8 tape does. The {ffffff} field here is filled from local
# receipt-time microseconds so lines stay strictly orderable, but it is
# NOT the exchange's true tick timestamp the way the C# source is.

def get_raw_data_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, label, RAW_DATA_FILENAME)


def get_level2_path(label: str) -> str:
    return os.path.join(BASE_DATA_DIR, label, LEVEL2_FILENAME)


def append_pipe_line(path: str, line: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def write_l1_line(label: str, line: str):
    """Appends one 'L1;' tape line to <label>/RawData.csv."""
    append_pipe_line(get_raw_data_path(label), line)


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
    microseconds, matching FormatNt8Timestamp() in the C# source."""
    date_part = dt.strftime("%Y%m%d%H%M%S")
    micros = micros_override if micros_override is not None else dt.microsecond
    frac_part = f"{micros:06d}"
    return date_part, frac_part


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
    """Handles a SymbolUpdate (trade/quote) message -> emits an 'L1;' line
    for each new trade, written to the L1-only files. Per-trade size is
    derived from the delta in Fyers' cumulative 'vol_traded_today' field
    (Fyers doesn't reliably expose a per-tick 'ltq' on this feed in all
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
    date_part, frac_part = format_nt8_timestamp(trade_dt, micros_override=recv_micros)

    if FYERS_SPLIT_PRINTS:
        # Matches the C# script's SplitPrints: emit `lots` separate lines,
        # each with volume=1, all sharing the same timestamp (same trade
        # moment) -- so a 5-lot print becomes 5 lines instead of 1.
        lots = max(1, int(round(size)))
        for _ in range(lots):
            line = f"L1;{side};{date_part};{frac_part};{fmt_num(ltp)};1"
            write_l1_line(INSTRUMENT_LABEL, line)
    else:
        line = f"L1;{side};{date_part};{frac_part};{fmt_num(ltp)};{fmt_num(size)}"
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
    `folder_name` (e.g. "tcs") sitting at Drive root — no date subfolders."""
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

    logger.info(f"Starting real-time L1+L2 collection for {FYERS_SYMBOL}...")
    await asyncio.gather(
        run_websocket_with_retry(),
        google_drive_sync_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Collector stopped manually.")
