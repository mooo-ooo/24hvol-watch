# blofin-vol-ws

Audits the 24h volume BloFin publishes for perpetual contracts by rebuilding it
from the public matched-trade stream (public WebSocket, no API key needed).

Three numbers are compared, all in contracts:

| Name | Source |
|---|---|
| **Published** | `vol24h` from the WebSocket `tickers` channel |
| **Matched** | Sum of every trade received on the WebSocket `trades` channel over the last 24h |
| **Candles** | Sum of the last 1440 one-minute candles from `GET /api/v1/market/candles` |

Columns:

- **Coverage**: share of the last 24h during which the trade stream was connected.
  `Matched vs published` is only shown once coverage is 100% (24h uninterrupted).
  Data survives restarts (`data/trades.json`), but the downtime is recorded as a gap.
- **Matched vs candles**: minute-by-minute check that needs no 24h wait. For every
  closed minute the stream fully covered, the trades received are compared with that
  minute's candle volume. This shows quickly whether the trade tape supports the
  published numbers.
- **Candles vs published**: whether the 24h ticker equals the sum of BloFin's own candles.

How to read the result: if `Matched vs candles` stays near 0% while candles and
published agree, the published volume matches the public trades. A large negative
gap means BloFin reports more volume than it shows as public trades. A gap of about
-50% suggests both sides of each trade are counted.

## Install
```bash
npm install
npm install -g pm2        # if not installed yet
```

## Configuration (`config.json`)
| Key | Meaning |
|---|---|
| `baseTokens` | Tokens to track, e.g. `["ONDO","SOL","BTC"]` |
| `quote` | Quote currency, default `USDT` → market `ONDO-USDT` |
| `logIntervalSec` | How often the table is printed to the log |
| `alertPct` / `alertWindowMin` | Alert when published 24h volume changes by ≥ X% within N minutes (0 = off) |
| `csvFile` / `csvIntervalSec` | CSV history file and write interval |
| `stateFile` | JSON file with the latest comparison |
| `tradesFile` | Saved per-minute trade totals and coverage (restored on restart) |
| `candleCheckIntervalSec` | How often 1m candles are fetched for comparison (0 = off) |
| `httpPort` | Live dashboard port (0 = off) |

pm2 restarts the process when `config.json` changes.

## Run
```bash
pm2 start ecosystem.config.js
pm2 logs blofin-vol          # watch the table
pm2 stop blofin-vol / pm2 restart blofin-vol
pm2 save && pm2 startup      # start again after reboot
```
Open http://localhost:3088 for a dashboard that refreshes every 2 seconds (JSON: `/api`).

Without pm2: `node index.js`
