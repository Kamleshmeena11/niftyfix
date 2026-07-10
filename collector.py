import json, os, sys, time, datetime, threading, queue, requests, pyotp
from urllib import parse
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

# =====================================================================
# CONFIGURATION — loaded from environment variables (GitHub Secrets).
# Never hardcode credentials in this file.
# =====================================================================
def _require_env(name):
    val = os.environ.get(name)
    if not val:
        print(f"❌ Missing required environment variable: {name}")
        sys.exit(1)
    return val

APP_ID          = _require_env("FYERS_APP_ID")
APP_TYPE        = _require_env("FYERS_APP_TYPE")
SECRET_KEY      = _require_env("FYERS_SECRET_KEY")
FY_ID           = _require_env("FYERS_FY_ID")
TOTP_SECRET     = _require_env("FYERS_TOTP_SECRET")
PIN             = _require_env("FYERS_PIN")
REDIRECT_URI    = os.environ.get("FYERS_REDIRECT_URI", "https://127.0.0.1/")

GOOGLE_CLIENT_ID      = _require_env("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET  = _require_env("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN  = _require_env("GOOGLE_REFRESH_TOKEN")

CLIENT_ID       = f"{APP_ID}-{APP_TYPE}"
SYMBOL          = "NSE:NIFTY50-INDEX"

DRIVE_SYNC_INTERVAL_SECONDS = 5
GOOGLE_SCOPES   = ["https://www.googleapis.com/auth/drive.file"]
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID") or None

MARKET_OPEN_IST_HOUR    = 9
MARKET_OPEN_IST_MINUTE  = 15
MARKET_CLOSE_IST_HOUR   = 15
MARKET_CLOSE_IST_MINUTE = 35
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# =====================================================================
# FILE LAYOUT
#   daily_bars/candles_1s_2026-07-08.csv   <- one file per trading day
#   candles_1s_all.csv                     <- every day's rows, combined,
#                                              lives outside the folder
# =====================================================================
DAILY_FOLDER_NAME = "daily_bars"
DRIVE_DAILY_SUBFOLDER_NAME = "daily_bars"   # matching sub-folder on Drive

TODAY_STR   = datetime.datetime.now(IST).strftime("%Y-%m-%d")
DAILY_FILE  = os.path.join(DAILY_FOLDER_NAME, f"candles_1s_{TODAY_STR}.csv")
COMBINED_FILE = "candles_1s_all.csv"

CSV_HEADER = "Timestamp,Open,High,Low,Close,Volume\n"

# Log file for tracking gaps / dropped-tick diagnostics separately from stdout
GAP_LOG_FILE = os.path.join(DAILY_FOLDER_NAME, f"gaps_{TODAY_STR}.log")

os.makedirs(DAILY_FOLDER_NAME, exist_ok=True)

BASE_URL              = "https://api-t2.fyers.in/vagator/v2"
BASE_URL_2            = "https://api-t1.fyers.in/api/v3"
URL_SEND_LOGIN_OTP    = BASE_URL   + "/send_login_otp"
URL_VERIFY_TOTP       = BASE_URL   + "/verify_otp"
URL_VERIFY_PIN        = BASE_URL   + "/verify_pin"
URL_TOKEN             = BASE_URL_2 + "/token"

SUCCESS, ERROR = 1, -1
fyers_ws   = None
start_time = time.time()

# =====================================================================
# RUNTIME — auto-calculate seconds until 15:35 IST
# =====================================================================
def compute_max_runtime_seconds():
    now_ist   = datetime.datetime.now(IST)
    close_ist = now_ist.replace(
        hour=MARKET_CLOSE_IST_HOUR, minute=MARKET_CLOSE_IST_MINUTE,
        second=0, microsecond=0
    )
    remaining = (close_ist - now_ist).total_seconds()
    return int(remaining) if remaining > 0 else 60

def wait_until_market_open():
    now_ist   = datetime.datetime.now(IST)
    open_ist  = now_ist.replace(
        hour=MARKET_OPEN_IST_HOUR, minute=MARKET_OPEN_IST_MINUTE,
        second=0, microsecond=0
    )
    close_ist = now_ist.replace(
        hour=MARKET_CLOSE_IST_HOUR, minute=MARKET_CLOSE_IST_MINUTE,
        second=0, microsecond=0
    )
    if now_ist >= close_ist:
        print(f"⛔ Market already closed for today. Exiting.")
        sys.exit(0)
    if now_ist < open_ist:
        wait_seconds = (open_ist - now_ist).total_seconds()
        print(f"⏳ Waiting {int(wait_seconds)}s for market open …")
        time.sleep(wait_seconds)
    else:
        print(f"▶️  Market already open — starting immediately.")

# =====================================================================
# GAP / DIAGNOSTIC LOGGING
# =====================================================================
def log_gap(text):
    """Writes to both stdout and a dedicated gap log so mismatches vs
    Fyers' own 1s bars can be traced back to a cause (reconnect vs
    missing exch_feed_time vs queue backlog) after the fact."""
    line = f"[{datetime.datetime.now(IST).strftime('%H:%M:%S')}] {text}"
    print(line)
    try:
        with open(GAP_LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# =====================================================================
# TICK QUEUE — decouples websocket receipt from bar-building/disk I/O
# =====================================================================
# FIX: on_message previously did bar aggregation *and* two blocking
# file writes per bar directly inside the websocket callback thread.
# If ticks arrive faster than that thread can drain them (or the OS
# socket buffer briefly stalls behind slow I/O), the underlying socket
# library can silently drop frames — no exception, no log, just a
# missing tick that skews that second's H/L versus Fyers' own bar.
#
# Now on_message does the absolute minimum: timestamp + queue.put().
# All aggregation/writing happens on a separate consumer thread that
# can never block the network thread.
tick_queue = queue.Queue()

# =====================================================================
# 1-SECOND CANDLE STATE (only touched by the consumer thread now)
# =====================================================================
current_bar_second = None
o = h = l = c = None
bar_start_vol = None
last_vol      = None
last_seen_second = None  # for gap detection across consecutive bars


def _write_bar(second, o_, h_, l_, c_, vol_):
    utc_dt = datetime.datetime.fromtimestamp(second, tz=datetime.timezone.utc)
    ist_dt = utc_dt.astimezone(IST)
    ts  = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

    row = f"{ts},{o_},{h_},{l_},{c_},{vol_}\n"

    with open(DAILY_FILE, "a") as f:
        f.write(row)
    with open(COMBINED_FILE, "a") as f:
        f.write(row)

    print(f"🕐 1s Candle: {row.strip()}")


def _start_new_bar(second, price, vol):
    global current_bar_second, o, h, l, c, bar_start_vol, last_vol
    current_bar_second = second
    o = h = l = c = price
    bar_start_vol = vol
    last_vol      = vol


def _flush_current_bar():
    global current_bar_second, o, h, l, c, bar_start_vol, last_vol
    if current_bar_second is None:
        return
    bar_volume = (
        (last_vol - bar_start_vol)
        if (last_vol is not None and bar_start_vol is not None)
        else 0
    )
    if bar_volume < 0:
        bar_volume = 0
    _write_bar(current_bar_second, o, h, l, c, bar_volume)
    current_bar_second = None


def _process_tick(price, vol, tick_second):
    """Bar-building logic, now run only on the consumer thread."""
    global o, h, l, c, last_vol, bar_start_vol, current_bar_second, last_seen_second

    # Gap detection: if the incoming second jumps by more than 1 from
    # the last one we processed, log it so mismatches can be traced.
    if last_seen_second is not None and tick_second > last_seen_second + 1:
        missing = tick_second - last_seen_second - 1
        log_gap(f"⚠️  Gap detected: {missing} second(s) with no ticks "
                 f"between {last_seen_second} and {tick_second}.")
    last_seen_second = tick_second

    if current_bar_second is None:
        _start_new_bar(tick_second, price, vol)
        return

    if tick_second == current_bar_second:
        h        = max(h, price)
        l        = min(l, price)
        c        = price
        last_vol = vol
    elif tick_second > current_bar_second:
        bar_volume = (
            (last_vol - bar_start_vol)
            if (last_vol is not None and bar_start_vol is not None)
            else 0
        )
        if bar_volume < 0:
            bar_volume = 0
        _write_bar(current_bar_second, o, h, l, c, bar_volume)
        _start_new_bar(tick_second, price, vol)
    else:
        # Out-of-order tick (arrived late, belongs to an already-closed
        # bar). Previously this was impossible to hit because on_message
        # only ever compared against "==" vs "else"; a late tick would
        # have silently reopened a new bar in the past. Now we just log
        # and drop it rather than corrupt the current bar.
        log_gap(f"⚠️  Late/out-of-order tick for second {tick_second} "
                 f"arrived after bar {current_bar_second} was already open — dropped.")


def bar_builder_loop():
    """Consumer thread: pulls ticks off the queue and builds bars.
    Runs independently of the websocket thread so slow disk I/O here
    can never cause the network thread to back up or drop frames."""
    while True:
        message = tick_queue.get()
        if message is None:  # sentinel for shutdown
            break
        price = message["ltp"]
        vol = message.get("vol_traded_today", 0)
        exch_ts = message.get("exch_feed_time")
        if exch_ts is None:
            # FIX: previously fell back to last_traded_time or
            # time.time() here, which mixes timestamp sources within
            # one session and misfiles ticks into the wrong second
            # bucket (looks exactly like a H/L mismatch vs Fyers).
            log_gap(f"⚠️  Tick missing exch_feed_time, dropped to avoid "
                     f"bucket corruption: {message}")
            continue
        _process_tick(price, vol, int(exch_ts))

# =====================================================================
# WEBSOCKET CALLBACKS
# =====================================================================
def on_message(message):
    # FIX: this callback now does nothing but validate + enqueue.
    # No aggregation, no file I/O, so it can never block the socket
    # thread regardless of tick rate.
    if "ltp" not in message or message["ltp"] is None:
        return
    tick_queue.put(message)


def on_error(message):
    print(f"⚠️  WS Error: {message}")


def on_close(message):
    log_gap("🔌 Connection closed.")
    if time.time() - start_time >= MAX_RUNTIME_SECONDS:
        return
    log_gap("🔌 Reconnecting in 5 s — any ticks during this window will be missing "
             "and may cause that second's bar to differ from Fyers' own data.")
    time.sleep(5)
    try:
        fyers_ws.connect()
    except Exception as e:
        print(f"⚠️  Reconnect failed: {e}")


def on_open():
    fyers_ws.subscribe(symbols=[SYMBOL], data_type="SymbolUpdate")
    print("✅ Live feed connected & subscribed.")

# =====================================================================
# WATCHDOG — kills process at market close
# =====================================================================
def runtime_watchdog():
    time.sleep(MAX_RUNTIME_SECONDS)
    print("⏱️  Session end reached — flushing last candle + final Drive sync …")
    # Give the consumer thread a moment to drain any queued ticks
    # before we flush, so the final bar isn't missing late-arriving data.
    time.sleep(1)
    _flush_current_bar()
    try:
        sync_all_to_drive()
    except Exception as e:
        print(f"⚠️  Final Drive sync failed: {e}")
    os._exit(0)

# =====================================================================
# GOOGLE DRIVE SYNC
# =====================================================================
def get_drive_service():
    creds = Credentials(
        token=None, refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
        scopes=GOOGLE_SCOPES,
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


_drive_service_cache = {}
_drive_daily_folder_id_cache = {}

_daily_safe_to_sync = False
_combined_safe_to_sync = False


def _get_service():
    if "service" not in _drive_service_cache:
        _drive_service_cache["service"] = get_drive_service()
    return _drive_service_cache["service"]


def _reset_drive_service():
    """Discards the cached Drive service/connection so the next call
    builds a brand new one."""
    _drive_service_cache.clear()


def _execute_with_retry(build_request_fn, max_retries=3, what=""):
    """Runs a single Drive API call (a service.files()....() request)
    with retries, rebuilding the Drive service from scratch between
    attempts.

    FIX: googleapiclient/httplib2 can reuse a connection that just
    finished a resumable upload for the *next* API call. If that
    connection's read state gets desynced, the next call — even a
    trivially valid one like `name = 'x.csv' and trashed = false` —
    comes back as a 400 "malformed request" from Google, even though
    nothing is wrong with the query itself. This was showing up as an
    intermittent failure syncing candles_1s_all.csv right after the
    daily file's resumable upload. Discarding and rebuilding the
    service on failure forces a fresh connection and reliably clears it.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        service = _get_service()
        try:
            return build_request_fn(service).execute()
        except Exception as e:
            last_err = e
            label = f" ({what})" if what else ""
            print(f"⚠️  Drive API call failed{label}, attempt {attempt}/{max_retries}: {e}")
            _reset_drive_service()
            time.sleep(1.5 * attempt)
    raise last_err


def _find_drive_file_id(filename, parent_id):
    """Safely builds the Google Drive search string to avoid 400 Bad Requests."""
    query_parts = [f"name = '{filename}'", "trashed = false"]

    # Only append parent condition if parent_id is valid, non-empty, and not literal 'None' string
    if parent_id and str(parent_id).strip() and str(parent_id).strip() != "None":
        query_parts.append(f"'{str(parent_id).strip()}' in parents")

    query = " and ".join(query_parts)

    def _req(service):
        return service.files().list(q=query, spaces="drive", fields="files(id, name)")

    results = _execute_with_retry(_req, what=f"find '{filename}'")
    files = results.get("files", [])
    return files[0]["id"] if files else None


def _get_or_create_daily_subfolder():
    if "id" in _drive_daily_folder_id_cache:
        return _drive_daily_folder_id_cache["id"]

    query_parts = [
        f"name = '{DRIVE_DAILY_SUBFOLDER_NAME}'",
        "trashed = false",
        "mimeType = 'application/vnd.google-apps.folder'"
    ]
    if DRIVE_FOLDER_ID and str(DRIVE_FOLDER_ID).strip() and str(DRIVE_FOLDER_ID).strip() != "None":
        query_parts.append(f"'{str(DRIVE_FOLDER_ID).strip()}' in parents")

    query = " and ".join(query_parts)

    def _list_req(service):
        return service.files().list(q=query, spaces="drive", fields="files(id, name)")

    results = _execute_with_retry(_list_req, what="find daily subfolder")
    files = results.get("files", [])

    if files:
        folder_id = files[0]["id"]
    else:
        metadata = {
            "name": DRIVE_DAILY_SUBFOLDER_NAME,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if DRIVE_FOLDER_ID and str(DRIVE_FOLDER_ID).strip() and str(DRIVE_FOLDER_ID).strip() != "None":
            metadata["parents"] = [str(DRIVE_FOLDER_ID).strip()]

        def _create_req(service):
            return service.files().create(body=metadata, fields="id")

        folder = _execute_with_retry(_create_req, what="create daily subfolder")
        folder_id = folder["id"]
        print(f"📁 Created Drive folder '{DRIVE_DAILY_SUBFOLDER_NAME}'.")

    _drive_daily_folder_id_cache["id"] = folder_id
    return folder_id


def upload_or_update_drive(local_path, parent_id):
    filename = os.path.basename(local_path)
    existing_id = _find_drive_file_id(filename, parent_id)

    def _req(service):
        # Built fresh inside the retry closure each attempt — a
        # MediaFileUpload object shouldn't be reused across a failed
        # and retried request since its internal stream position may
        # have already advanced.
        media = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
        if existing_id:
            return service.files().update(fileId=existing_id, media_body=media)
        metadata = {"name": filename}
        if parent_id and str(parent_id).strip() and str(parent_id).strip() != "None":
            metadata["parents"] = [str(parent_id).strip()]
        return service.files().create(body=metadata, media_body=media, fields="id")

    _execute_with_retry(_req, what=f"upload '{filename}'")
    print(f"☁️  Synced '{filename}' → Google Drive.")


def sync_all_to_drive():
    global _daily_safe_to_sync, _combined_safe_to_sync
    daily_folder_id = _get_or_create_daily_subfolder()

    if _daily_safe_to_sync:
        upload_or_update_drive(DAILY_FILE, daily_folder_id)
    else:
        print("⏸️  Skipping daily-file sync — not yet confirmed safe (retrying bootstrap).")
        try:
            result = _download_from_drive(
                os.path.basename(DAILY_FILE), daily_folder_id, DAILY_FILE
            )
            _daily_safe_to_sync = True
            print(f"⬇️  Daily file now confirmed ({result}).")
        except Exception as e:
            print(f"⚠️  Still can't confirm daily file: {e}")

    if _combined_safe_to_sync:
        upload_or_update_drive(COMBINED_FILE, DRIVE_FOLDER_ID)
    else:
        print("⏸️  Skipping combined-file sync — not yet confirmed safe (retrying bootstrap).")
        try:
            result = _download_from_drive(
                os.path.basename(COMBINED_FILE), DRIVE_FOLDER_ID, COMBINED_FILE
            )
            _combined_safe_to_sync = True
            print(f"⬇️  Combined file now confirmed ({result}).")
        except Exception as e:
            print(f"⚠️  Still can't confirm combined file: {e}")


def _download_from_drive(filename, parent_id, local_path):
    file_id = _find_drive_file_id(filename, parent_id)
    if not file_id:
        return "none_found"

    def _do_download():
        service = _get_service()
        request = service.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    last_err = None
    for attempt in range(1, 4):
        try:
            data = _do_download()
            with open(local_path, "wb") as f:
                f.write(data)
            return "downloaded"
        except Exception as e:
            last_err = e
            print(f"⚠️  Drive download of '{filename}' failed, attempt {attempt}/3: {e}")
            _reset_drive_service()
            time.sleep(1.5 * attempt)
    raise last_err


def bootstrap_local_files():
    global _daily_safe_to_sync, _combined_safe_to_sync

    try:
        daily_folder_id = _get_or_create_daily_subfolder()

        result = _download_from_drive(
            os.path.basename(DAILY_FILE), daily_folder_id, DAILY_FILE
        )
        _daily_safe_to_sync = True
        print(f"⬇️  Daily file: {result} (safe to sync).")
    except Exception as e:
        print(f"⚠️  Could not verify daily file against Drive yet: {e} — will retry, NOT syncing until confirmed.")

    try:
        result = _download_from_drive(
            os.path.basename(COMBINED_FILE), DRIVE_FOLDER_ID, COMBINED_FILE
        )
        _combined_safe_to_sync = True
        print(f"⬇️  Combined file: {result} (safe to sync).")
    except Exception as e:
        print(f"⚠️  Could not verify combined file against Drive yet: {e} — will retry, NOT syncing until confirmed.")

    for _f in (DAILY_FILE, COMBINED_FILE):
        if not os.path.exists(_f):
            with open(_f, "w") as fh:
                fh.write(CSV_HEADER)


def drive_sync_loop():
    while not (os.path.exists(DAILY_FILE) and os.path.exists(COMBINED_FILE)):
        time.sleep(2)
    while True:
        try:
            sync_all_to_drive()
        except Exception as e:
            print(f"⚠️  Drive sync error: {e}")
        time.sleep(DRIVE_SYNC_INTERVAL_SECONDS)

# =====================================================================
# FYERS TOTP AUTO-LOGIN
# =====================================================================
def send_login_otp():
    try:
        r = requests.post(URL_SEND_LOGIN_OTP, json={"fy_id": FY_ID, "app_id": "2"}, timeout=15)
        r.raise_for_status()
        return SUCCESS, r.json()["request_key"]
    except Exception as e:
        return ERROR, str(e)


def verify_totp(request_key):
    try:
        otp = pyotp.TOTP(TOTP_SECRET).now()
        r   = requests.post(URL_VERIFY_TOTP, json={"request_key": request_key, "otp": otp}, timeout=15)
        r.raise_for_status()
        return SUCCESS, r.json()["request_key"]
    except Exception as e:
        return ERROR, str(e)


def verify_pin(request_key):
    try:
        payload = {"request_key": request_key, "identity_type": "pin", "identifier": PIN}
        r = requests.post(URL_VERIFY_PIN, json=payload, timeout=15)
        r.raise_for_status()
        return SUCCESS, r.json()["data"]["access_token"]
    except Exception as e:
        return ERROR, str(e)


def get_auth_code(trade_access_token):
    try:
        payload = {
            "fyers_id": FY_ID, "app_id": APP_ID, "redirect_uri": REDIRECT_URI,
            "appType": APP_TYPE, "code_challenge": "", "state": "sample_state",
            "scope": "", "nonce": "", "response_type": "code", "create_cookie": True,
        }
        headers = {"Authorization": f"Bearer {trade_access_token}"}
        r = requests.post(URL_TOKEN, json=payload, headers=headers, timeout=15)
        if r.status_code != 308:
            return ERROR, r.text
        url       = r.json()["Url"]
        auth_code = parse.parse_qs(parse.urlparse(url).query)["auth_code"][0]
        return SUCCESS, auth_code
    except Exception as e:
        return ERROR, str(e)


def totp_login():
    max_retries = 5
    for attempt in range(max_retries):
        print(f"🔑 Running fully automatic TOTP login (Attempt {attempt + 1}/{max_retries}) …")

        status, request_key = send_login_otp()
        if status != SUCCESS:
            print(f"⚠️ send_login_otp failed: {request_key}")
            time.sleep(10)
            continue

        status, request_key = verify_totp(request_key)
        if status != SUCCESS:
            print(f"⚠️ verify_totp failed: {request_key}")
            time.sleep(10)
            continue

        status, trade_token = verify_pin(request_key)
        if status != SUCCESS:
            print(f"⚠️ verify_pin failed: {trade_token}")
            time.sleep(10)
            continue

        status, auth_code = get_auth_code(trade_token)
        if status != SUCCESS:
            print(f"⚠️ get_auth_code failed: {auth_code}")
            time.sleep(10)
            continue

        session = fyersModel.SessionModel(
            client_id=CLIENT_ID, secret_key=SECRET_KEY, redirect_uri=REDIRECT_URI,
            response_type="code", grant_type="authorization_code",
        )
        session.set_token(auth_code)
        response = session.generate_token()

        if "access_token" not in response:
            print(f"⚠️ generate_token failed: {response}")
            time.sleep(10)
            continue

        print("✅ Login successful.")
        return response["access_token"]

    print("❌ All login attempts failed after maximum retries.")
    sys.exit(1)

# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    wait_until_market_open()
    MAX_RUNTIME_SECONDS = compute_max_runtime_seconds()
    start_time = time.time()
    print(f"⏱️  Auto-stop in {MAX_RUNTIME_SECONDS}s.")
    print(f"📄 Today's file: {DAILY_FILE}")
    print(f"📄 Combined file: {COMBINED_FILE}")
    print(f"📄 Gap log: {GAP_LOG_FILE}")

    token = totp_login()

    bootstrap_local_files()

    threading.Thread(target=drive_sync_loop,  daemon=True).start()
    threading.Thread(target=runtime_watchdog, daemon=True).start()
    threading.Thread(target=bar_builder_loop, daemon=True).start()

    fyers_ws = data_ws.FyersDataSocket(
        access_token=f"{CLIENT_ID}:{token}",
        log_path=os.getcwd(),
        litemode=False,
        on_connect=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    fyers_ws.connect()
