'use strict';
/**
 * BloFin perpetual 24h volume auditor.
 *
 * Independently rebuilds 24h volume from the public matched-trade stream and
 * compares it with the volume BloFin publishes:
 *   1. Ticker `vol24h` (WebSocket "tickers" channel)          -> "published"
 *   2. Sum of matched trades (WebSocket "trades" channel)      -> "matched"
 *   3. Sum of 1m candles over 24h (REST /market/candles)       -> "candles"
 *
 * The matched total is only a full 24h figure once the tracker has run for
 * 24h without interruption ("coverage" = 100%). Before that, the per-minute
 * check (matched trades vs. 1m candle volume, on fully covered minutes only)
 * already tells you whether the trade tape agrees with the published numbers.
 *
 * Outputs: console table, data/latest.json, data/trades.json, data/vol24h.csv,
 * live dashboard at http://localhost:<httpPort> (JSON: /api).
 * Requires Node.js >= 18.
 */
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');

// ---------- config ----------
const CONFIG_PATH = process.env.CONFIG || path.join(__dirname, 'config.json');
const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
const QUOTE = (cfg.quote || 'USDT').toUpperCase();
const SYMBOLS = [...new Set((cfg.baseTokens || []).map(b => `${String(b).trim().toUpperCase()}-${QUOTE}`))];
if (!SYMBOLS.length) { console.error('config.json: baseTokens must contain at least one token'); process.exit(1); }

const WS_URL = cfg.wsUrl || 'wss://openapi.blofin.com/ws/public';
const REST_URL = cfg.restUrl || 'https://openapi.blofin.com';
const LOG_SEC = cfg.logIntervalSec ?? 30;
const ALERT_PCT = cfg.alertPct ?? 0;
const ALERT_WIN_MS = (cfg.alertWindowMin ?? 60) * 60e3;
const CSV_FILE = path.resolve(__dirname, cfg.csvFile || 'data/vol24h.csv');
const CSV_SEC = cfg.csvIntervalSec ?? 60;
const STATE_FILE = path.resolve(__dirname, cfg.stateFile || 'data/latest.json');
const TRADES_FILE = path.resolve(__dirname, cfg.tradesFile || 'data/trades.json');
const CANDLE_SEC = cfg.candleCheckIntervalSec ?? 300;
const HTTP_PORT = cfg.httpPort ?? 0;

const DAY_MS = 24 * 60 * 60e3;
const MIN_MS = 60e3;
const DEDUP_MS = 15 * MIN_MS;      // trade IDs are remembered this long (covers reconnect replays)
const SAVE_MS = 10e3;

// ---------- helpers ----------
const num = v => (v === undefined || v === null || v === '' ? null : Number(v));
const ts = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const log = (...a) => console.log(`[${ts()}]`, ...a);
const fmtUsd = x => x == null || isNaN(x) ? 'n/a'
  : Math.abs(x) >= 1e9 ? `$${(x / 1e9).toFixed(2)}B`
  : Math.abs(x) >= 1e6 ? `$${(x / 1e6).toFixed(2)}M`
  : Math.abs(x) >= 1e3 ? `$${(x / 1e3).toFixed(1)}K` : `$${x.toFixed(0)}`;
const fmtNum = x => x == null || isNaN(x) ? 'n/a' : Math.round(x).toLocaleString('en-US');
const pct = x => x == null || isNaN(x) ? '-' : `${x >= 0 ? '+' : ''}${x.toFixed(2)}%`;
const diffPct = (a, b) => (a == null || !b ? null : (a / b - 1) * 100);
const floorMin = t => Math.floor(t / MIN_MS) * MIN_MS;
const ensureDir = f => fs.mkdirSync(path.dirname(f), { recursive: true });
const writeJsonAtomic = (file, obj, pretty) => {
  ensureDir(file);
  fs.writeFileSync(file + '.tmp', JSON.stringify(obj, null, pretty ? 2 : 0));
  fs.renameSync(file + '.tmp', file);
};

// ---------- state ----------
const contractValue = {};          // instId -> base currency per contract
const ticker = {};                 // instId -> latest published ticker metrics
const volHistory = {};             // instId -> [{t, v}] one sample per minute, for alerts
const lastAlert = {};
const buckets = {};                // instId -> Map(minute -> {n, contracts, quote, buyContracts, sellContracts})
const seenTrades = new Map();      // `${instId}:${tradeId}` -> receive time
const candleCheck = {};            // instId -> latest candle comparison
let coverage = [];                 // [{start, end}] periods when the trade stream was live
let liveSegment = null;            // current open coverage segment
let warnedFields = false;
SYMBOLS.forEach(s => { volHistory[s] = []; buckets[s] = new Map(); });

async function getJson(url) {
  const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
  const j = await r.json();
  if (j.code !== '0') throw new Error(`${url}: ${j.msg || j.code}`);
  return j.data;
}

async function loadInstruments() {
  try {
    for (const ins of await getJson(`${REST_URL}/api/v1/market/instruments`)) {
      if (SYMBOLS.includes(ins.instId)) contractValue[ins.instId] = num(ins.contractValue);
    }
    const missing = SYMBOLS.filter(s => !(s in contractValue));
    if (missing.length) log(`WARN markets not found on BloFin (check token names): ${missing.join(', ')}`);
  } catch (e) {
    log('WARN could not load instruments; USDT notionals will be unavailable:', e.message);
  }
}

// ---------- published ticker ----------
function onTicker(t) {
  const s = t.instId;
  if (!SYMBOLS.includes(s)) return;
  const last = num(t.last);
  const volContracts = num(t.vol24h);
  let volBase = num(t.volCurrency24h);
  if (volBase == null && volContracts != null && contractValue[s]) volBase = volContracts * contractValue[s];
  const volUsdt = volBase != null && last ? volBase * last : null;
  if (volContracts == null && !warnedFields) {
    warnedFields = true;
    log('WARN ticker volume fields are missing. Fields received:', Object.keys(t).join(', '));
  }
  const open = num(t.open24h);
  ticker[s] = {
    last, open24h: open, high24h: num(t.high24h), low24h: num(t.low24h),
    vol24hContracts: volContracts, vol24hBase: volBase, vol24hUsdt: volUsdt,
    chg24hPct: diffPct(last, open), exchTs: num(t.ts), updatedAt: Date.now(),
  };

  // Alert when the published 24h volume moves sharply within the alert window.
  const h = volHistory[s], now = Date.now();
  if (volUsdt != null && (!h.length || now - h[h.length - 1].t >= MIN_MS)) h.push({ t: now, v: volUsdt });
  while (h.length && now - h[0].t > ALERT_WIN_MS + 2 * MIN_MS) h.shift();
  const ref = h.find(p => now - p.t <= ALERT_WIN_MS);
  const ready = h.length && now - h[0].t >= ALERT_WIN_MS - MIN_MS;
  const d = ready && ref ? diffPct(volUsdt, ref.v) : null;
  ticker[s].volChgWindowPct = d;
  if (ALERT_PCT && d != null && Math.abs(d) >= ALERT_PCT && now - (lastAlert[s] || 0) > 10 * MIN_MS) {
    lastAlert[s] = now;
    log(`ALERT ${s}: published 24h volume changed ${pct(d)} in ${cfg.alertWindowMin ?? 60}m (${fmtUsd(volUsdt)})`);
  }
}

// ---------- matched trades ----------
function onTrade(t) {
  const s = t.instId;
  if (!SYMBOLS.includes(s)) return;
  const id = t.tradeId == null ? null : String(t.tradeId);
  const size = num(t.size), price = num(t.price), time = num(t.ts);
  if (!id || !(size > 0) || !(price > 0) || !time) return;

  const key = `${s}:${id}`;
  if (seenTrades.has(key)) return;
  seenTrades.set(key, Date.now());

  const minute = floorMin(time);
  const map = buckets[s];
  let b = map.get(minute);
  if (!b) map.set(minute, b = { n: 0, contracts: 0, quote: 0, buyContracts: 0, sellContracts: 0 });
  b.n += 1;
  b.contracts += size;
  if (contractValue[s] != null) b.quote += size * contractValue[s] * price;
  if (t.side === 'buy') b.buyContracts += size;
  else if (t.side === 'sell') b.sellContracts += size;
}

function prune(now = Date.now()) {
  const cutoff = now - DAY_MS - 5 * MIN_MS;
  for (const s of SYMBOLS) for (const m of buckets[s].keys()) if (m < cutoff) buckets[s].delete(m);
  for (const [k, t] of seenTrades) { if (now - t > DEDUP_MS) seenTrades.delete(k); else break; }
  coverage = coverage.filter(c => c.end >= cutoff);
}

// ---------- coverage (when was the trade stream actually live?) ----------
function allSegments(now = Date.now()) {
  return liveSegment ? [...coverage, { start: liveSegment.start, end: now }] : coverage;
}
function coveredMs(from, to, now) {
  let ms = 0;
  for (const c of allSegments(now)) ms += Math.max(0, Math.min(to, c.end) - Math.max(from, c.start));
  return ms;
}
// A minute counts as fully covered only if the stream was live for the whole minute.
const minuteCovered = (m, now) => coveredMs(m, m + MIN_MS, now) >= MIN_MS - 1;

// ---------- comparison ----------
function snapshot(s, now = Date.now()) {
  const from = now - DAY_MS;
  const sum = { n: 0, contracts: 0, quote: 0, buyContracts: 0, sellContracts: 0 };
  for (const [m, b] of buckets[s]) {
    if (m + MIN_MS <= from) continue;
    for (const k in sum) sum[k] += b[k];
  }
  const cov = coveredMs(from, now, now) / DAY_MS * 100;
  const t = ticker[s] || {};
  const c = candleCheck[s] || {};
  const hasCv = contractValue[s] != null;
  return {
    symbol: s,
    last: t.last ?? null,
    chg24hPct: t.chg24hPct ?? null,
    volChgWindowPct: t.volChgWindowPct ?? null,
    // published by BloFin
    publishedVol24hContracts: t.vol24hContracts ?? null,
    publishedVol24hUsdt: t.vol24hUsdt ?? null,
    // rebuilt from public matched trades
    matchedTradeCount24h: sum.n,
    matchedVol24hContracts: sum.contracts,
    matchedVol24hUsdt: hasCv ? sum.quote : null,
    matchedBuyContracts: sum.buyContracts,
    matchedSellContracts: sum.sellContracts,
    coveragePct: Math.min(100, cov),
    complete: cov >= 99.9,
    // matched vs published (only meaningful when complete)
    matchedVsPublishedPct: diffPct(sum.contracts, t.vol24hContracts),
    // REST 1m candles
    candleVol24hContracts: c.candleVol24hContracts ?? null,
    candleVsPublishedPct: diffPct(c.candleVol24hContracts, t.vol24hContracts),
    // per-minute check on fully covered minutes: matched trades vs candle volume
    checkedMinutes: c.minutes ?? 0,
    checkedCandleContracts: c.candleContracts ?? null,
    checkedMatchedContracts: c.matchedContracts ?? null,
    matchedVsCandlesPct: diffPct(c.matchedContracts, c.candleContracts),
    worstMinutes: c.worst || [],
    candleCheckedAt: c.at ?? null,
  };
}
const snapshotAll = () => { const now = Date.now(); return SYMBOLS.map(s => snapshot(s, now)); };

async function runCandleCheck() {
  const now = Date.now();
  for (const s of SYMBOLS) {
    if (Object.keys(contractValue).length && !(s in contractValue)) continue;
    try {
      const rows = await getJson(`${REST_URL}/api/v1/market/candles?instId=${s}&bar=1m&limit=1440`);
      // row: [ts, open, high, low, close, vol(contracts), volCurrency, volCurrencyQuote, confirm]
      let vol24 = 0, minutes = 0, candleContracts = 0, matchedContracts = 0;
      const per = [];
      for (const r of rows) {
        // Field 5 (contracts) is rounded to whole numbers; derive contracts from base volume when possible.
        const m = Number(r[0]), v = contractValue[s] ? Number(r[6]) / contractValue[s] : Number(r[5]);
        if (m + MIN_MS > now - DAY_MS) vol24 += v;
        // Compare only closed minutes that the trade stream fully covered (2s grace for late trades).
        if (m + MIN_MS + 2000 > now || !minuteCovered(m, now)) continue;
        const mine = buckets[s].get(m)?.contracts || 0;
        minutes++; candleContracts += v; matchedContracts += mine;
        if (Math.abs(mine - v) > 1e-9) per.push({ minute: new Date(m).toISOString(), candle: v, matched: mine, diff: mine - v });
      }
      per.sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));
      candleCheck[s] = { at: now, candleVol24hContracts: vol24, minutes, candleContracts, matchedContracts, mismatchedMinutes: per.length, worst: per.slice(0, 5) };
    } catch (e) {
      log(`WARN candle check failed for ${s}:`, e.message);
    }
  }
}

// ---------- persistence ----------
function loadSaved() {
  try {
    if (!fs.existsSync(TRADES_FILE)) return;
    const saved = JSON.parse(fs.readFileSync(TRADES_FILE, 'utf8'));
    for (const s of SYMBOLS) for (const [m, b] of saved.buckets?.[s] || []) buckets[s].set(m, b);
    coverage = saved.coverage || [];
    prune();
    const mins = SYMBOLS.reduce((a, s) => a + buckets[s].size, 0);
    log(`Restored ${mins} minute buckets and ${coverage.length} coverage segments from ${path.relative(__dirname, TRADES_FILE)}`);
  } catch (e) {
    log('WARN could not restore saved trades:', e.message);
  }
}

function save() {
  const now = Date.now();
  prune(now);
  const out = {};
  for (const s of SYMBOLS) out[s] = [...buckets[s].entries()];
  // Coverage is saved up to "now", matching exactly the trades saved in this file.
  writeJsonAtomic(TRADES_FILE, { savedAt: now, coverage: allSegments(now), buckets: out });
  writeJsonAtomic(STATE_FILE, { updatedAt: new Date(now).toISOString(), ws: wsStatus(), symbols: snapshotAll() }, true);
}

// ---------- websocket ----------
let ws, pingTimer, watchdog, retry = 0, lastMsgAt = 0, stopping = false;
const wsStatus = () => (ws?.readyState === WebSocket.OPEN ? 'connected' : 'disconnected');

function closeSegment() {
  if (!liveSegment) return;
  // Close at the last message seen: anything after that may have been missed.
  coverage.push({ start: liveSegment.start, end: Math.max(liveSegment.start, lastMsgAt) });
  liveSegment = null;
}

function connect() {
  ws = new WebSocket(WS_URL);
  let pendingTradeAcks = 0;
  ws.on('open', () => {
    retry = 0; lastMsgAt = Date.now();
    const subs = Object.keys(contractValue).length ? SYMBOLS.filter(s => s in contractValue) : SYMBOLS;
    pendingTradeAcks = subs.length;
    log(`WebSocket connected; subscribing to tickers + trades: ${subs.join(', ')}`);
    ws.send(JSON.stringify({
      op: 'subscribe',
      args: subs.flatMap(instId => [{ channel: 'tickers', instId }, { channel: 'trades', instId }]),
    }));
    pingTimer = setInterval(() => ws.readyState === WebSocket.OPEN && ws.send('ping'), 20e3);
    watchdog = setInterval(() => {
      if (Date.now() - lastMsgAt > 60e3) { log('WARN no data for 60s, reconnecting'); ws.terminate(); }
    }, 15e3);
  });
  ws.on('message', buf => {
    lastMsgAt = Date.now();
    const str = buf.toString();
    if (str === 'pong') return;
    let m; try { m = JSON.parse(str); } catch { return; }
    if (m.event === 'error') return log('WebSocket error event:', str);
    if (m.event === 'subscribe') {
      // Coverage starts once every trades subscription is acknowledged.
      if (m.arg?.channel === 'trades' && --pendingTradeAcks === 0 && !liveSegment) {
        liveSegment = { start: Date.now() };
        log('Trade stream live; coverage segment started');
      }
      return;
    }
    if (m.event) return;
    if (!Array.isArray(m.data)) return;
    if (m.arg?.channel === 'tickers') m.data.forEach(onTicker);
    else if (m.arg?.channel === 'trades') m.data.forEach(onTrade);
  });
  ws.on('close', code => {
    clearInterval(pingTimer); clearInterval(watchdog);
    closeSegment();
    if (stopping) return;
    const wait = Math.min(30e3, 1000 * 2 ** retry++);
    log(`WebSocket closed (${code}); coverage gap started; reconnecting in ${wait / 1000}s`);
    setTimeout(connect, wait);
  });
  ws.on('error', e => log('WebSocket error:', e.message));
}

// ---------- output ----------
function printTable() {
  const rows = snapshotAll().filter(r => r.publishedVol24hContracts != null);
  if (!rows.length) return log('Waiting for market data...');
  const head = ['Symbol', 'Published 24h', 'Matched 24h', 'Coverage', 'Matched/Pub', 'Candles/Pub', 'Chk min', 'Matched/Candles'];
  const w = [12, 16, 16, 10, 13, 13, 9, 16];
  const line = cols => cols.map((c, i) => (i ? String(c).padStart(w[i]) : String(c).padEnd(w[i]))).join('');
  const body = rows.map(r => line([
    r.symbol, fmtNum(r.publishedVol24hContracts), fmtNum(r.matchedVol24hContracts),
    `${r.coveragePct.toFixed(1)}%`, r.complete ? pct(r.matchedVsPublishedPct) : '(partial)',
    pct(r.candleVsPublishedPct), r.checkedMinutes, pct(r.matchedVsCandlesPct),
  ]));
  log(`Volumes in contracts\n${line(head)}\n${body.join('\n')}`);
}

function appendCsv() {
  ensureDir(CSV_FILE);
  const cols = ['time', 'symbol', 'last', 'published_vol24h_contracts', 'published_vol24h_usdt',
    'matched_trade_count_24h', 'matched_vol24h_contracts', 'matched_vol24h_usdt', 'coverage_pct', 'complete',
    'matched_vs_published_pct', 'candle_vol24h_contracts', 'candle_vs_published_pct',
    'checked_minutes', 'checked_candle_contracts', 'checked_matched_contracts', 'matched_vs_candles_pct'];
  if (!fs.existsSync(CSV_FILE)) fs.writeFileSync(CSV_FILE, cols.join(',') + '\n');
  const t = new Date().toISOString();
  const r4 = x => (x == null ? '' : +x.toFixed(4));
  const out = snapshotAll().filter(r => r.publishedVol24hContracts != null).map(r => [
    t, r.symbol, r.last, r.publishedVol24hContracts, r4(r.publishedVol24hUsdt),
    r.matchedTradeCount24h, r4(r.matchedVol24hContracts), r4(r.matchedVol24hUsdt), r4(r.coveragePct), r.complete,
    r.complete ? r4(r.matchedVsPublishedPct) : '', r4(r.candleVol24hContracts), r4(r.candleVsPublishedPct),
    r.checkedMinutes, r4(r.checkedCandleContracts), r4(r.checkedMatchedContracts), r4(r.matchedVsCandlesPct),
  ].join(','));
  if (out.length) fs.appendFileSync(CSV_FILE, out.join('\n') + '\n');
}

const PAGE = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BloFin Volume Audit</title><style>
body{font:14px system-ui,sans-serif;background:#0f1115;color:#e6e6e6;margin:16px}
.wrap{overflow-x:auto}table{border-collapse:collapse;width:100%}th,td{padding:8px 10px;border-bottom:1px solid #2a2e37;text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left}th{color:#9aa0aa;font-weight:500}.up{color:#3ccf91}.dn{color:#ff6b6b}.mut{color:#9aa0aa}small{color:#9aa0aa}
</style><h3>BloFin 24h Volume Audit</h3><div class=wrap><table><thead><tr><th>Symbol</th><th>Published 24h</th><th>Matched 24h</th>
<th>Coverage</th><th>Matched vs published</th><th>Candles vs published</th><th>Checked minutes</th><th>Matched vs candles</th><th>Trades</th></tr></thead>
<tbody id=b></tbody></table></div>
<p><small>Volumes in contracts. "Matched" is rebuilt from the public trade stream and only equals a full 24h once coverage reaches 100%.
"Matched vs candles" compares, minute by minute, the trades received with BloFin's 1m candle volume, on minutes the stream fully covered.</small></p>
<p><small id=u></small></p>
<script>
const n=x=>x==null?'n/a':Math.round(x).toLocaleString('en-US');
const p=x=>x==null?'<span class=mut>-</span>':'<span class='+(Math.abs(x)<0.5?'up':'dn')+'>'+(x>=0?'+':'')+x.toFixed(2)+'%</span>';
async function r(){try{const j=await (await fetch('api')).json();
document.getElementById('b').innerHTML=j.symbols.map(s=>'<tr><td>'+s.symbol+'</td><td>'+n(s.publishedVol24hContracts)+'</td><td>'+n(s.matchedVol24hContracts)+'</td><td>'+s.coveragePct.toFixed(1)+'%</td><td>'+(s.complete?p(s.matchedVsPublishedPct):'<span class=mut>partial</span>')+'</td><td>'+p(s.candleVsPublishedPct)+'</td><td>'+s.checkedMinutes+'</td><td>'+p(s.matchedVsCandlesPct)+'</td><td>'+n(s.matchedTradeCount24h)+'</td></tr>').join('');
document.getElementById('u').textContent='Updated '+new Date().toLocaleTimeString()+' | WebSocket '+j.ws;}catch(e){}}
r();setInterval(r,2000);
</script>`;

function startHttp() {
  if (!HTTP_PORT) return;
  http.createServer((req, res) => {
    if (req.url.startsWith('/api')) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ws: wsStatus(), coverage: allSegments(), symbols: snapshotAll() }));
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(PAGE);
  }).listen(HTTP_PORT, () => log(`Live dashboard: http://localhost:${HTTP_PORT}`));
}

function shutdown() {
  stopping = true;
  try { save(); } catch (e) { log('Could not save on shutdown:', e.message); }
  try { ws?.close(); } catch {}
  log('Stopped.');
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

(async () => {
  log(`Tracking ${SYMBOLS.length} markets: ${SYMBOLS.join(', ')}`);
  await loadInstruments();
  loadSaved();
  connect();
  startHttp();
  if (LOG_SEC) setInterval(printTable, LOG_SEC * 1000);
  setInterval(() => { try { save(); } catch (e) { log('Could not save state:', e.message); } }, SAVE_MS);
  if (CSV_SEC) setInterval(() => { try { appendCsv(); } catch (e) { log('Could not write CSV:', e.message); } }, CSV_SEC * 1000);
  if (CANDLE_SEC) {
    setTimeout(runCandleCheck, 90e3); // first check once at least one full minute has been covered
    setInterval(runCandleCheck, CANDLE_SEC * 1000);
  }
})();
