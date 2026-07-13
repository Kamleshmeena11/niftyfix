import datetime, os, time
from fyers_apiv3 import fyersModel
from collector import totp_login, CLIENT_ID, SYMBOL, IST

DAYS_BACK = 15
OUT_DIR = "history_1s"
os.makedirs(OUT_DIR, exist_ok=True)

token = totp_login()
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=token, log_path=os.getcwd())

today = datetime.datetime.now(IST).date()

for i in range(DAYS_BACK):
    day = today - datetime.timedelta(days=i)
    if day.weekday() >= 5:  # skip Sat/Sun
        continue
    d = day.strftime("%Y-%m-%d")

    resp = fyers.history(data={
        "symbol": SYMBOL,
        "resolution": "1S",
        "date_format": "1",
        "range_from": d,
        "range_to": d,
        "cont_flag": "1",
    })

    candles = resp.get("candles") or []
    if resp.get("s") != "ok" or not candles:
        print(f"❌ {d}: no data — response: {resp.get('s')}, {resp.get('message', '')}")
        time.sleep(1)
        continue

    out_file = os.path.join(OUT_DIR, f"fyers_1s_{d}.csv")
    with open(out_file, "w") as f:
        f.write("Timestamp,Open,High,Low,Close,Volume\n")
        for ts, o, h, l, c, v in candles:
            ist_ts = datetime.datetime.fromtimestamp(ts, tz=IST).strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{ist_ts},{o},{h},{l},{c},{v}\n")

    print(f"✅ {d}: {len(candles)} candles → {out_file}")
    time.sleep(1)  # respect rate limits

print("Done. Compare files in history_1s/ against your daily_bars/ CSVs.")
