'use strict';
/**
 * BloFin perp vol24h monitor (WebSocket, public channel "tickers").
 * - Danh sách token: config.json -> baseTokens (vd ["ONDO","SOL"]) + quote ("USDT")
 * - Tự reconnect + resubscribe, ping giữ kết nối
 * - Log định kỳ (xem bằng `pm2 logs`), cảnh báo khi vol 24h đổi mạnh
 * - Ghi data/latest.json + data/vol24h.csv
 * - Trang xem trực tiếp: http://localhost:<httpPort>  (JSON: /api)
 * Yêu cầu Node >= 18.
 */
const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');

const CONFIG_PATH = process.env.CONFIG || path.join(__dirname, 'config.json');
const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
const QUOTE = (cfg.quote || 'USDT').toUpperCase();
const SYMBOLS = [...new Set((cfg.baseTokens || []).map(b => `${String(b).trim().toUpperCase()}-${QUOTE}`))];
if (!SYMBOLS.length) { console.error('config.json: baseTokens trống'); process.exit(1); }

const WS_URL = cfg.wsUrl || 'wss://openapi.blofin.com/ws/public';
const REST_URL = cfg.restUrl || 'https://openapi.blofin.com';
const LOG_SEC = cfg.logIntervalSec ?? 30;
const ALERT_PCT = cfg.alertPct ?? 0;
const ALERT_WIN_MS = (cfg.alertWindowMin ?? 60) * 60e3;
const CSV_FILE = path.resolve(__dirname, cfg.csvFile || 'data/vol24h.csv');
const CSV_SEC = cfg.csvIntervalSec ?? 60;
const STATE_FILE = path.resolve(__dirname, cfg.stateFile || 'data/latest.json');
const HTTP_PORT = cfg.httpPort ?? 0;

const num = v => (v === undefined || v === null || v === '' ? null : Number(v));
const ts = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const log = (...a) => console.log(`[${ts()}]`, ...a);
const fmtUsd = x => x == null || isNaN(x) ? 'n/a'
  : Math.abs(x) >= 1e9 ? `$${(x / 1e9).toFixed(2)}B`
  : Math.abs(x) >= 1e6 ? `$${(x / 1e6).toFixed(2)}M`
  : Math.abs(x) >= 1e3 ? `$${(x / 1e3).toFixed(1)}K` : `$${x.toFixed(0)}`;
const pct = x => x == null || isNaN(x) ? '—' : `${x >= 0 ? '+' : ''}${x.toFixed(2)}%`;

// ---------- state ----------
const contractValue = {};            // instId -> coin per contract
const state = {};                    // instId -> latest metrics
const history = {};                  // instId -> [{t, v}] (1 mẫu/phút)
const lastAlert = {};
SYMBOLS.forEach(s => { history[s] = []; });
let warnedFields = false;

async function loadInstruments() {
  try {
    const r = await fetch(`${REST_URL}/api/v1/market/instruments`, { signal: AbortSignal.timeout(10000) });
    const j = await r.json();
    for (const ins of j.data || []) if (SYMBOLS.includes(ins.instId)) contractValue[ins.instId] = num(ins.contractValue);
    const missing = SYMBOLS.filter(s => !(s in contractValue));
    if (missing.length) log(`⚠️  Không thấy trên BloFin (kiểm tra tên token): ${missing.join(', ')}`);
  } catch (e) {
    log('⚠️  Không lấy được instruments (vẫn chạy tiếp):', e.message);
  }
}

function onTicker(t) {
  const s = t.instId;
  if (!SYMBOLS.includes(s)) return;
  const last = num(t.last);
  const volContracts = num(t.vol24h);
  let volBase = num(t.volCurrency24h);
  if (volBase == null && volContracts != null && contractValue[s]) volBase = volContracts * contractValue[s];
  const volUsdt = volBase != null && last ? volBase * last : null;
  if (volUsdt == null && !warnedFields) {
    warnedFields = true;
    log('⚠️  Không tìm thấy trường volume. Ticker trả về các trường:', Object.keys(t).join(', '));
  }
  const open = num(t.open24h);
  state[s] = {
    symbol: s, last, open24h: open, high24h: num(t.high24h), low24h: num(t.low24h),
    vol24hContracts: volContracts, vol24hBase: volBase, vol24hUsdt: volUsdt,
    chg24hPct: last && open ? (last / open - 1) * 100 : null,
    exchTs: num(t.ts), updatedAt: Date.now(),
  };
  // lấy mẫu 1 lần/phút để so sánh theo cửa sổ thời gian
  const h = history[s], now = Date.now();
  if (volUsdt != null && (!h.length || now - h[h.length - 1].t >= 60e3)) h.push({ t: now, v: volUsdt });
  while (h.length && now - h[0].t > ALERT_WIN_MS + 120e3) h.shift();
  const ref = h.find(p => now - p.t <= ALERT_WIN_MS);
  const ready = h.length && now - h[0].t >= ALERT_WIN_MS - 60e3;
  state[s].volChgWindowPct = ready && ref && volUsdt ? (volUsdt / ref.v - 1) * 100 : null;
  const d = state[s].volChgWindowPct;
  if (ALERT_PCT && d != null && Math.abs(d) >= ALERT_PCT && now - (lastAlert[s] || 0) > 10 * 60e3) {
    lastAlert[s] = now;
    log(`🚨 ALERT ${s}: vol 24h ${pct(d)} trong ${cfg.alertWindowMin ?? 60} phút (${fmtUsd(volUsdt)})`);
  }
}

// ---------- websocket ----------
let ws, pingTimer, watchdog, retry = 0, lastMsgAt = 0, stopping = false;

function connect() {
  ws = new WebSocket(WS_URL);
  ws.on('open', () => {
    retry = 0; lastMsgAt = Date.now();
    // bỏ các mã không tồn tại trên sàn (nếu đã tải được instruments) để tránh lỗi subscribe
    const subs = Object.keys(contractValue).length ? SYMBOLS.filter(s => s in contractValue) : SYMBOLS;
    log(`✅ WS connected, subscribe tickers: ${subs.join(', ')}`);
    ws.send(JSON.stringify({ op: 'subscribe', args: subs.map(instId => ({ channel: 'tickers', instId })) }));
    pingTimer = setInterval(() => ws.readyState === WebSocket.OPEN && ws.send('ping'), 20e3);
    watchdog = setInterval(() => {
      if (Date.now() - lastMsgAt > 60e3) { log('⚠️  60s không có dữ liệu, reconnect...'); ws.terminate(); }
    }, 15e3);
  });
  ws.on('message', buf => {
    lastMsgAt = Date.now();
    const s = buf.toString();
    if (s === 'pong') return;
    let m; try { m = JSON.parse(s); } catch { return; }
    if (m.event === 'error') return log('❌ WS error:', s);
    if (m.event) return; // subscribe ack...
    if (m.arg?.channel === 'tickers' && Array.isArray(m.data)) m.data.forEach(onTicker);
  });
  ws.on('close', (code) => {
    clearInterval(pingTimer); clearInterval(watchdog);
    if (stopping) return;
    const wait = Math.min(30e3, 1000 * 2 ** retry++);
    log(`WS closed (${code}), reconnect sau ${wait / 1000}s`);
    setTimeout(connect, wait);
  });
  ws.on('error', e => log('WS error:', e.message));
}

// ---------- output ----------
function printTable() {
  const rows = SYMBOLS.map(s => state[s]).filter(Boolean);
  if (!rows.length) return log('chưa có dữ liệu...');
  const lines = rows.map(r =>
    `${r.symbol.padEnd(12)}${String(r.last).padStart(12)}${fmtUsd(r.vol24hUsdt).padStart(12)}` +
    `${pct(r.volChgWindowPct).padStart(12)}${pct(r.chg24hPct).padStart(10)}`);
  log(`\n${'Symbol'.padEnd(12)}${'Last'.padStart(12)}${'Vol24h'.padStart(12)}${`ΔVol ${cfg.alertWindowMin ?? 60}m`.padStart(12)}${'Δ24h'.padStart(10)}\n` + lines.join('\n'));
}

function ensureDir(f) { fs.mkdirSync(path.dirname(f), { recursive: true }); }
function writeState() {
  ensureDir(STATE_FILE);
  const tmp = STATE_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify({ updatedAt: new Date().toISOString(), symbols: state }, null, 2));
  fs.renameSync(tmp, STATE_FILE);
}
function appendCsv() {
  ensureDir(CSV_FILE);
  if (!fs.existsSync(CSV_FILE)) fs.writeFileSync(CSV_FILE, 'time,symbol,last,vol24h_contracts,vol24h_base,vol24h_usdt,chg24h_pct\n');
  const t = new Date().toISOString();
  const out = SYMBOLS.map(s => state[s]).filter(Boolean)
    .map(r => [t, r.symbol, r.last, r.vol24hContracts, r.vol24hBase, r.vol24hUsdt?.toFixed(2), r.chg24hPct?.toFixed(4)].join(','));
  if (out.length) fs.appendFileSync(CSV_FILE, out.join('\n') + '\n');
}

const PAGE = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BloFin Vol24h</title><style>
body{font:14px system-ui,sans-serif;background:#0f1115;color:#e6e6e6;margin:16px}
table{border-collapse:collapse;width:100%;max-width:820px}th,td{padding:8px 10px;border-bottom:1px solid #2a2e37;text-align:right}
th:first-child,td:first-child{text-align:left}th{color:#9aa0aa;font-weight:500}.up{color:#3ccf91}.dn{color:#ff6b6b}small{color:#9aa0aa}
</style><h3>BloFin perp – Vol 24h</h3><table><thead><tr><th>Symbol</th><th>Last</th><th>Vol 24h (USDT)</th>
<th>ΔVol window</th><th>Δ24h</th><th>Updated</th></tr></thead><tbody id=b></tbody></table><p><small id=u></small></p>
<script>
const f=x=>x==null?'n/a':x>=1e9?'$'+(x/1e9).toFixed(2)+'B':x>=1e6?'$'+(x/1e6).toFixed(2)+'M':'$'+(x/1e3).toFixed(1)+'K';
const p=x=>x==null?'—':'<span class='+(x>=0?'up':'dn')+'>'+(x>=0?'+':'')+x.toFixed(2)+'%</span>';
async function r(){try{const j=await (await fetch('api')).json();
document.getElementById('b').innerHTML=Object.values(j.symbols).map(s=>'<tr><td>'+s.symbol+'</td><td>'+s.last+'</td><td>'+f(s.vol24hUsdt)+'</td><td>'+p(s.volChgWindowPct)+'</td><td>'+p(s.chg24hPct)+'</td><td>'+new Date(s.updatedAt).toLocaleTimeString()+'</td></tr>').join('');
document.getElementById('u').textContent='Cập nhật '+new Date().toLocaleTimeString()+' · WS '+j.ws;}catch(e){}}
r();setInterval(r,2000);
</script>`;

function startHttp() {
  if (!HTTP_PORT) return;
  http.createServer((req, res) => {
    if (req.url.startsWith('/api')) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ws: ws?.readyState === WebSocket.OPEN ? 'connected' : 'disconnected', symbols: state }));
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(PAGE);
  }).listen(HTTP_PORT, () => log(`🌐 Xem trực tiếp: http://localhost:${HTTP_PORT}`));
}

function shutdown() {
  stopping = true;
  try { writeState(); } catch {}
  try { ws?.close(); } catch {}
  log('Đã dừng.');
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

(async () => {
  log(`Theo dõi ${SYMBOLS.length} cặp: ${SYMBOLS.join(', ')}`);
  await loadInstruments();
  connect();
  startHttp();
  if (LOG_SEC) setInterval(printTable, LOG_SEC * 1000);
  setInterval(() => { try { writeState(); } catch (e) { log('write state lỗi:', e.message); } }, 5000);
  if (CSV_SEC) setInterval(() => { try { appendCsv(); } catch (e) { log('csv lỗi:', e.message); } }, CSV_SEC * 1000);
})();
