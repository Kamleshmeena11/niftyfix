import json, os, sys, time, datetime, threading, requests, pyotp
from urllib import parse
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

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
DATA_FILE       = "candles_1s.csv"
DRIVE_SYNC_INTERVAL_SECONDS = 5
GOOGLE_SCOPES   = ["https://www.googleapis.com/auth/drive.file"]
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID") or None

MARKET_OPEN_IST_HOUR    = 9
MARKET_OPEN_IST_MINUTE  = 15
MARKET_CLOSE_IST_HOUR   = 15
MARKET_CLOSE_IST_MINUTE = 35
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

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
#
# Logic (exactly as requested):
#   Open  = price of the FIRST tick seen in that second
#   Close = price of the LAST  tick seen in that second
#   High  = max price across all ticks in that second
#   Low   = min price across all ticks in that second
#
# A candle for second N is only known to be "finished" once a tick
# from a later second arrives — there's no other signal that no more
# trades are coming for second N. That one-tick lag before writing is
# unavoidable in a live stream (not a bug).
# =====================================================================
current_bar_second = None
o = h = l = c = None
bar_start_vol = None
last_vol      = None

if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w") as f:
        f.write("Timestamp,Open,High,Low,Close,Volume\n")


def _write_bar(second, o_, h_, l_, c_, vol_):
    ts  = datetime.datetime.fromtimestamp(second, IST).strftime("%Y-%m-%d %H:%M:%S")
    row = f"{ts},{o_},{h_},{l_},{c_},{vol_}\n"
    with open(DATA_FILE, "a") as f:
        f.write(row)
    print(f"🕐 1s Candle: {row.strip()}")


def _start_new_bar(second, price, vol):
    global current_bar_second, o, h, l, c, bar_start_vol, last_vol
    current_bar_second = second
    o = h = l = c = price          # Open = this (first) tick's price
    bar_start_vol = vol
    last_vol      = vol


def _flush_current_bar():
    """Write whatever candle is currently open. Called on shutdown so the
    last second of the session isn't silently dropped."""
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

    vol = message.get("vol_traded_today", 0)   # correct field name; 0/absent is normal for an index

    exch_ts = message.get("exch_feed_time") or message.get("last_traded_time")
    if not exch_ts:
        # Only happens if litemode=True strips these fields — keep
        # litemode=False (below) so this fallback should never trigger.
        exch_ts = time.time()
    tick_second = int(exch_ts)

    if current_bar_second is None:
        # First tick of the whole session — just opens the bucket.
        _start_new_bar(tick_second, price, vol)
        return

    if tick_second == current_bar_second:
        # Still inside the same second → fold this tick into the open candle.
        h        = max(h, price)          # High = max seen this second
        l        = min(l, price)          # Low  = min seen this second
        c        = price                  # Close = latest tick's price
        last_vol = vol
    else:
        # A tick from a new second arrived → the previous second is done.
        bar_volume = (
            (last_vol - bar_start_vol)
            if (last_vol is not None and bar_start_vol is not None)
            else 0
        )
        _write_bar(current_bar_second, o, h, l, c, bar_volume)
        _start_new_bar(tick_second, price, vol)   # this tick opens the new candle


def on_error(message):
    print(f"⚠️  WS Error: {message}")


def on_close(message):
    _flush_current_bar()   # don't lose the last (still-open) candle
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
        upload_or_update_drive(DATA_FILE)
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


def _find_drive_file_id(service, filename):
    query = f"name = '{filename}' and trashed = false"
    if DRIVE_FOLDER_ID:
        query += f" and '{DRIVE_FOLDER_ID}' in parents"
    results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = results.get("files", [])
    return files[0]["id"] if files else None


_drive_service_cache = {}

def upload_or_update_drive(local_path):
    if "service" not in _drive_service_cache:
        _drive_service_cache["service"] = get_drive_service()
    service  = _drive_service_cache["service"]
    filename = os.path.basename(local_path)
    media    = MediaFileUpload(local_path, mimetype="text/csv", resumable=True)
    existing_id = _find_drive_file_id(service, filename)
    if existing_id:
        service.files().update(fileId=existing_id, media_body=media).execute()
    else:
        metadata = {"name": filename}
        if DRIVE_FOLDER_ID:
            metadata["parents"] = [DRIVE_FOLDER_ID]
        service.files().create(body=metadata, media_body=media, fields="id").execute()
    print(f"☁️  Synced '{filename}' → Google Drive.")


def drive_sync_loop():
    while not os.path.exists(DATA_FILE):
        time.sleep(2)
    while True:
        try:
            upload_or_update_drive(DATA_FILE)
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

    token = totp_login()

    threading.Thread(target=drive_sync_loop,  daemon=True).start()
    threading.Thread(target=runtime_watchdog, daemon=True).start()

    fyers_ws = data_ws.FyersDataSocket(
        access_token=f"{CLIENT_ID}:{token}",
        log_path=os.getcwd(),
        litemode=False,   # required — full mode is what provides exch_feed_time / vol_traded_today
        on_connect=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    fyers_ws.connect()
