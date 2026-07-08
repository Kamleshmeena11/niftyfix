import json, os, sys, time, datetime, threading, requests, pyotp
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
# 1-SECOND CANDLE STATE
# =====================================================================
current_bar_second = None
o = h = l = c = None
bar_start_vol = None
last_vol      = None


def _write_bar(second, o_, h_, l_, c_, vol_):
    # Convert epoch timestamp to strict UTC first, then project to IST
    utc_dt = datetime.datetime.fromtimestamp(second, tz=datetime.timezone.utc)
    ist_dt = utc_dt.astimezone(IST)
    ts  = ist_dt.strftime("%Y-%m-%d %H:%M:%S")

    row = f"{ts},{o_},{h_},{l_},{c_},{vol_}\n"

    # Write to BOTH the day's own file and the running combined file
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
    global current_bar_second
    if current_bar_second is None:
        return
    bar_volume = (
        (last_vol - bar_start_vol)
        if (last_vol is not None and bar_start_vol is not None)
        else 0
    )
    _write_bar(current_bar_second, o, h, l, c, bar_volume)
    current_bar_second = None

# =====================================================================
# WEBSOCKET CALLBACKS
# =====================================================================
def on_message(message):
    global o, h, l, c, last_vol

    if "ltp" not in message:
        return
    price = message["ltp"]
    if price is None:
        return

    vol = message.get("vol_traded_today", 0)

    exch_ts = message.get("exch_feed_time") or message.get("last_traded_time")
    if not exch_ts:
        exch_ts = time.time()
    tick_second = int(exch_ts)

    if current_bar_second is None:
        _start_new_bar(tick_second, price, vol)
        return

    if tick_second == current_bar_second:
        h        = max(h, price)
        l        = min(l, price)
        c        = price
        last_vol = vol
    else:
        bar_volume = (
            (last_vol - bar_start_vol)
            if (last_vol is not None and bar_start_vol is not None)
            else 0
        )
        _write_bar(current_bar_second, o, h, l, c, bar_volume)
        _start_new_bar(tick_second, price, vol)


def on_error(message):
    print(f"⚠️  WS Error: {message}")


def on_close(message):
    _flush_current_bar()
    if time.time() - start_time >= MAX_RUNTIME_SECONDS:
        return
    print("🔌 Connection closed — reconnecting in 5 s …")
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

# Safety flags: sync_all_to_drive() must NEVER upload a file that could be
# missing prior history. These start False and are only set True once we're
# CERTAIN the local file's contents are safe to push (either we successfully
# downloaded what was already there, or we confirmed nothing exists yet).
_daily_safe_to_sync = False
_combined_safe_to_sync = False


def _get_service():
    if "service" not in _drive_service_cache:
        _drive_service_cache["service"] = get_drive_service()
    return _drive_service_cache["service"]


def _find_drive_file_id(service, filename, parent_id):
    query = f"name = '{filename}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


def _get_or_create_daily_subfolder(service):
    """Finds (or creates) the Drive sub-folder that mirrors DAILY_FOLDER_NAME,
    nested under DRIVE_FOLDER_ID (or Drive root if that isn't set)."""
    if "id" in _drive_daily_folder_id_cache:
        return _drive_daily_folder_id_cache["id"]

    query = (
        f"name = '{DRIVE_DAILY_SUBFOLDER_NAME}' and trashed = false "
        f"and mimeType = 'application/vnd.google-apps.folder'"
    )
    if DRIVE_FOLDER_ID:
        query += f" and '{DRIVE_FOLDER_ID}' in parents"
    results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = results.get("files", [])

    if files:
        folder_id = files[0]["id"]
    else:
        metadata = {
            "name": DRIVE_DAILY_SUBFOLDER_NAME,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if DRIVE_FOLDER_ID:
            metadata["parents"] = [DRIVE_FOLDER_ID]
        folder = service.files().create(body=metadata, fields="id").execute()
        folder_id = folder["id"]
        print(f"📁 Created Drive folder '{DRIVE_DAILY_SUBFOLDER_NAME}'.")

    _drive_daily_folder_id_cache["id"] = folder_id
    return folder_id


def upload_or_update_drive(local_path, parent_id):
    service  = _get_service()
    filename = os.path.basename(local_path)
    media    = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    existing_id = _find_drive_file_id(service, filename, parent_id)
    if existing_id:
        service.files().update(fileId=existing_id, media_body=media).execute()
    else:
        metadata = {"name": filename}
        if parent_id:
            metadata["parents"] = [parent_id]
        service.files().create(body=metadata, media_body=media, fields="id").execute()
    print(f"☁️  Synced '{filename}' → Google Drive.")


def sync_all_to_drive():
    global _daily_safe_to_sync, _combined_safe_to_sync
    service = _get_service()
    daily_folder_id = _get_or_create_daily_subfolder(service)

    if _daily_safe_to_sync:
        upload_or_update_drive(DAILY_FILE, daily_folder_id)
    else:
        print("⏸️  Skipping daily-file sync — not yet confirmed safe (retrying bootstrap).")
        try:
            result = _download_from_drive(
                service, os.path.basename(DAILY_FILE), daily_folder_id, DAILY_FILE
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
                service, os.path.basename(COMBINED_FILE), DRIVE_FOLDER_ID, COMBINED_FILE
            )
            _combined_safe_to_sync = True
            print(f"⬇️  Combined file now confirmed ({result}).")
        except Exception as e:
            print(f"⚠️  Still can't confirm combined file: {e}")


def _download_from_drive(service, filename, parent_id, local_path):
    """Pulls an existing file down from Drive into local_path.
    Returns 'downloaded' if a file was found and pulled down,
    'none_found' if Drive confirmed no such file exists (safe — nothing to
    lose), or raises an exception if the check itself failed (unsafe —
    caller must NOT treat this as safe to overwrite)."""
    file_id = _find_drive_file_id(service, filename, parent_id)
    if not file_id:
        return "none_found"
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    with open(local_path, "wb") as f:
        f.write(buf.getvalue())
    return "downloaded"


def bootstrap_local_files():
    """Runs once at startup. GitHub Actions checks out a clean repo every run,
    so local CSVs start empty. If we blindly let the sync step push that empty
    local file to Drive, it would erase everything already stored there. So:
    for each file, either confirm we pulled down the existing Drive copy, or
    confirm (via a successful, error-free check) that Drive truly has nothing
    yet. Only in those two cases is it marked safe to sync. If the Drive
    check itself errors out, we do NOT fall back to a fresh/empty file for
    that target — we keep retrying in the background sync loop until we can
    verify it safely, so we never risk overwriting real history with a stub."""
    global _daily_safe_to_sync, _combined_safe_to_sync

    try:
        service = _get_service()
        daily_folder_id = _get_or_create_daily_subfolder(service)

        result = _download_from_drive(
            service, os.path.basename(DAILY_FILE), daily_folder_id, DAILY_FILE
        )
        _daily_safe_to_sync = True
        print(f"⬇️  Daily file: {result} (safe to sync).")
    except Exception as e:
        print(f"⚠️  Could not verify daily file against Drive yet: {e} — will retry, NOT syncing until confirmed.")

    try:
        service = _get_service()
        result = _download_from_drive(
            service, os.path.basename(COMBINED_FILE), DRIVE_FOLDER_ID, COMBINED_FILE
        )
        _combined_safe_to_sync = True
        print(f"⬇️  Combined file: {result} (safe to sync).")
    except Exception as e:
        print(f"⚠️  Could not verify combined file against Drive yet: {e} — will retry, NOT syncing until confirmed.")

    # Only create a fresh header-only file locally if we've CONFIRMED (not
    # assumed) there's nothing on Drive to lose, or the flag is still pending
    # and the file simply doesn't exist locally yet so writers have something
    # to append to. This local stub is harmless by itself — it only becomes
    # risky if uploaded before the safety flag is set, which sync_all_to_drive
    # now guards against directly.
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
    print("🔑 Running fully automatic TOTP login …")
    status, request_key = send_login_otp()
    if status != SUCCESS: print(f"❌ send_login_otp failed: {request_key}"); sys.exit(1)
    status, request_key = verify_totp(request_key)
    if status != SUCCESS: print(f"❌ verify_totp failed: {request_key}"); sys.exit(1)
    status, trade_token = verify_pin(request_key)
    if status != SUCCESS: print(f"❌ verify_pin failed: {trade_token}"); sys.exit(1)
    status, auth_code = get_auth_code(trade_token)
    if status != SUCCESS: print(f"❌ get_auth_code failed: {auth_code}"); sys.exit(1)

    session = fyersModel.SessionModel(
        client_id=CLIENT_ID, secret_key=SECRET_KEY, redirect_uri=REDIRECT_URI,
        response_type="code", grant_type="authorization_code",
    )
    session.set_token(auth_code)
    response = session.generate_token()
    if "access_token" not in response:
        print(f"❌ generate_token failed: {response}"); sys.exit(1)
    print("✅ Login successful.")
    return response["access_token"]

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

    token = totp_login()

    bootstrap_local_files()

    threading.Thread(target=drive_sync_loop,  daemon=True).start()
    threading.Thread(target=runtime_watchdog, daemon=True).start()

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
