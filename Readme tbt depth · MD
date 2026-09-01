# 50-level depth: what changed and what to check first

## The one thing to confirm before running this live
Fyers' 50-level order book is **only available over their separate TBT
("Versova") protobuf feed** — `wss://rtsocket-api.fyers.in/versova`. The
standard `data_ws` `DepthUpdate` feed this script already used caps out at a
handful of levels for equities no matter what `DOM_LEVELS` is set to; there's
no server-side knob to get more out of it.

**As of Fyers' own docs/community posts, TBT is currently available for NFO
(NSE Futures & Options) instruments only — not NSE cash-market equities.**
`NSE:TCS-EQ` is a cash-market symbol. If that restriction still holds when
you run this, you'll likely get a connection/auth rejection or empty depth on
that symbol specifically. Worth a quick confirmation with Fyers support, or
pointing `FYERS_SYMBOL` at the TCS futures contract if 50-level depth matters
more than trading the cash symbol.

## What was added
- `FYERS_DEPTH_SOURCE=standard` (default, unchanged behavior) or `tbt`.
- When `tbt`: the standard socket only takes trades (L1); a second
  connection (`run_tbt_depth_with_retry`) handles depth via the real TBT
  protobuf feed and writes to the same `Level2.csv` in the same `L2;...`
  format as before, so nothing downstream needs to change.
- `msg.proto` / `msg_pb2.py`: the actual Fyers TBT protobuf schema (verified
  against Fyers' public reference implementation, not guessed), compiled and
  round-trip tested.

## Setup
```
pip install websockets protobuf --break-system-packages
```
`msg_pb2.py` is already compiled and included — keep it next to
`collector.py`. Set `FYERS_DEPTH_SOURCE=tbt` in your environment to switch on.

## Correctness note
TBT sends a full order-book snapshot only on the first message per symbol,
then incremental per-level updates after that. The depth handler keeps a
persistent 50-slot book per side and only emits a line for levels actually
present in each message — it does not treat "not mentioned in this update" as
"level dropped." I verified this against simulated snapshot + incremental
messages using the real protobuf schema; I have not been able to test it
against a live Fyers TBT connection (no credentials here), so treat the first
live run as a validation pass — watch the first few minutes of `Level2.csv`
output against what you'd expect before trusting it unattended.
