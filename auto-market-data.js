// ---------------------------------------------------------------------------
// Auto Market Data (AMD) — long-history monthly reference index
//
// AMD publishes 3 lines, all rebased to 1000 at Jan 2020:
//   - Overall  (headline used-vehicle wholesale value index)
//   - EV       (electric vehicle sub-index)
//   - Non-EV   (non-electric vehicle sub-index)
//
// Data origin: industry-standard wholesale used-vehicle value index (monthly).
// Source XLSX files are parsed into a seed JSON shipped in data/seed/. Nothing
// labelled "Manheim" or "Cox" is exposed to the frontend — all UI surfaces say
// "Auto Market Data". Source URLs are kept only for the monthly refresh cron.
//
// Base 1000 at Jan 2020 is our transformation of the public source; see
// Auto_Market_Index_Methodology v2.1 — not directly comparable to externally
// published values.
// ---------------------------------------------------------------------------

const fs = require('fs');
const path = require('path');
const https = require('https');

const SEED_FILE = path.join(__dirname, 'data', 'seed', 'auto_market_data_seed.json');

// ---------- Schema ----------
function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS auto_market_data_monthly (
      date TEXT PRIMARY KEY,
      index_overall_1000 REAL,
      index_ev_1000 REAL,
      index_nonev_1000 REAL,
      src_overall REAL,
      src_ev REAL,
      src_nonev REAL,
      mom_pct_overall REAL,
      yoy_pct_overall REAL,
      mom_pct_ev REAL,
      yoy_pct_ev REAL,
      mom_pct_nonev REAL,
      yoy_pct_nonev REAL,
      updated_at TEXT NOT NULL
    )
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_amd_date ON auto_market_data_monthly(date)');

  // Migration: if an older schema exists without the new columns, the CREATE
  // TABLE IF NOT EXISTS above is a no-op. Add any missing columns defensively.
  const cols = new Set(db.prepare("PRAGMA table_info(auto_market_data_monthly)").all().map(r => r.name));
  const needed = [
    ['index_overall_1000',  'REAL'],
    ['index_ev_1000',       'REAL'],
    ['index_nonev_1000',    'REAL'],
    ['src_overall',         'REAL'],
    ['src_ev',              'REAL'],
    ['src_nonev',           'REAL'],
    ['mom_pct_overall',     'REAL'],
    ['yoy_pct_overall',     'REAL'],
    ['mom_pct_ev',          'REAL'],
    ['yoy_pct_ev',          'REAL'],
    ['mom_pct_nonev',       'REAL'],
    ['yoy_pct_nonev',       'REAL'],
  ];
  for (const [name, type] of needed) {
    if (!cols.has(name)) {
      try { db.exec(`ALTER TABLE auto_market_data_monthly ADD COLUMN ${name} ${type}`); } catch (e) {}
    }
  }
}

// ---------- Seed from shipped JSON on first run ----------
// If the table is empty, load everything. If non-empty but missing the new
// 3-line columns, re-seed with OR REPLACE to upgrade in place.
function seedIfEmpty(db) {
  if (!fs.existsSync(SEED_FILE)) {
    console.warn(`[auto-market-data] seed file missing: ${SEED_FILE}`);
    return { seeded: false, existing: 0 };
  }

  const existing = db.prepare('SELECT COUNT(*) AS c FROM auto_market_data_monthly').get().c;
  const missingNewCols = db.prepare(`
    SELECT COUNT(*) AS c FROM auto_market_data_monthly
    WHERE index_overall_1000 IS NOT NULL
  `).get().c;

  // Skip seeding only if we already have rows AND the new columns are populated.
  if (existing > 0 && missingNewCols > 0) {
    return { seeded: false, existing };
  }

  const seed = JSON.parse(fs.readFileSync(SEED_FILE, 'utf8'));
  const now = new Date().toISOString();
  const ins = db.prepare(`
    INSERT OR REPLACE INTO auto_market_data_monthly
      (date, index_overall_1000, index_ev_1000, index_nonev_1000,
       src_overall, src_ev, src_nonev,
       mom_pct_overall, yoy_pct_overall,
       mom_pct_ev, yoy_pct_ev,
       mom_pct_nonev, yoy_pct_nonev,
       updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const tx = db.transaction(rows => {
    for (const r of rows) {
      const date = String(r.date).slice(0, 10); // YYYY-MM-DD
      ins.run(
        date,
        numOrNull(r.index_overall_1000),
        numOrNull(r.index_ev_1000),
        numOrNull(r.index_nonev_1000),
        numOrNull(r.src_overall),
        numOrNull(r.src_ev),
        numOrNull(r.src_nonev),
        numOrNull(r.mom_pct_overall),
        numOrNull(r.yoy_pct_overall),
        numOrNull(r.mom_pct_ev),
        numOrNull(r.yoy_pct_ev),
        numOrNull(r.mom_pct_nonev),
        numOrNull(r.yoy_pct_nonev),
        now,
      );
    }
  });
  tx(seed);
  const finalCount = db.prepare('SELECT COUNT(*) AS c FROM auto_market_data_monthly').get().c;
  console.log(`[auto-market-data] seeded ${finalCount} monthly rows (3-line: Overall/EV/Non-EV, base 1000 at Jan 2020)`);
  return { seeded: true, existing: finalCount };
}

function numOrNull(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// ---------- Query helpers ----------

// Return all 3 lines for a since-date. Each line is an array of {timestamp,value}.
function getSeries(db, sinceIso) {
  const sinceDate = sinceIso ? sinceIso.slice(0, 10) : '1900-01-01';
  const rows = db.prepare(`
    SELECT date, index_overall_1000, index_ev_1000, index_nonev_1000
    FROM auto_market_data_monthly
    WHERE date >= ?
    ORDER BY date ASC
  `).all(sinceDate);

  const overall = [];
  const ev = [];
  const nonev = [];
  for (const r of rows) {
    const ts = r.date + 'T00:00:00.000Z';
    if (r.index_overall_1000 != null) overall.push({ timestamp: ts, value: r.index_overall_1000 });
    if (r.index_ev_1000     != null)       ev.push({ timestamp: ts, value: r.index_ev_1000 });
    if (r.index_nonev_1000  != null)    nonev.push({ timestamp: ts, value: r.index_nonev_1000 });
  }
  return { overall, ev, nonev };
}

function getLatest(db) {
  const r = db.prepare(`
    SELECT date,
           index_overall_1000, index_ev_1000, index_nonev_1000,
           mom_pct_overall, yoy_pct_overall,
           mom_pct_ev, yoy_pct_ev,
           mom_pct_nonev, yoy_pct_nonev
    FROM auto_market_data_monthly
    ORDER BY date DESC LIMIT 1
  `).get();
  if (!r) return null;
  return {
    date: r.date,
    overall: { value: r.index_overall_1000, mom_pct: r.mom_pct_overall, yoy_pct: r.yoy_pct_overall },
    ev:      { value: r.index_ev_1000,      mom_pct: r.mom_pct_ev,      yoy_pct: r.yoy_pct_ev },
    nonev:   { value: r.index_nonev_1000,   mom_pct: r.mom_pct_nonev,   yoy_pct: r.yoy_pct_nonev },
  };
}

// ---------- Monthly refresh (fetch latest XLSX when available) ----------
//
// Note: in v2.1 refresh is handled by the Auto-2 quarterly ingestion pipeline
// (Phase D.5). The skeleton below remains for the cron path but is not wired
// to a parser for the new 3-line schema yet.

const MONTHS_SHORT = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MONTHS_FULL  = ['January','February','March','April','May','June','July','August','September','October','November','December'];

function httpGetBuffer(url, timeoutMs = 15000) {
  return new Promise((resolve) => {
    const req = https.get(url, { timeout: timeoutMs }, res => {
      if (res.statusCode !== 200) { res.resume(); return resolve({ ok: false, status: res.statusCode }); }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ ok: true, buf: Buffer.concat(chunks) }));
    });
    req.on('error', () => resolve({ ok: false }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, timeout: true }); });
  });
}

function candidateUrls(dataYear, dataMonth) {
  let pubMonth = dataMonth + 1, pubYear = dataYear;
  if (pubMonth > 12) { pubMonth = 1; pubYear += 1; }
  const pubMM = String(pubMonth).padStart(2, '0');
  const short = MONTHS_SHORT[dataMonth - 1];
  const full  = MONTHS_FULL[dataMonth - 1];
  const base = `https://www.coxautoinc.com/wp-content/uploads/${pubYear}/${pubMM}`;
  const suffix = 'Manheim-Used-Vehicle-Value-Index.xlsx';
  return [
    `${base}/${full}-${dataYear}-${suffix}`,
    `${base}/${short}-${dataYear}-${suffix}`,
  ];
}

async function refreshLatest(db, xlsxParser) {
  const latest = getLatest(db);
  if (!latest) return 'no existing data; run seed first';
  const [yStr, mStr] = latest.date.split('-');
  let year = Number(yStr), month = Number(mStr);
  const tried = [];
  for (let step = 1; step <= 2; step++) {
    month += 1; if (month > 12) { month = 1; year += 1; }
    const urls = candidateUrls(year, month);
    for (const url of urls) {
      const r = await httpGetBuffer(url);
      tried.push(`${url} → ${r.ok ? 'OK' : r.status || 'err'}`);
      if (r.ok) {
        const tmp = path.join(__dirname, 'data', 'seed', `_latest-${year}-${String(month).padStart(2,'0')}.xlsx`);
        fs.writeFileSync(tmp, r.buf);
        try {
          const added = await xlsxParser(db, tmp);
          fs.unlinkSync(tmp);
          return `fetched ${url}; added ${added} new rows`;
        } catch (e) {
          return `fetched ${url} but parse failed: ${e.message}`;
        }
      }
    }
  }
  return `no new XLSX found. Probed: ${tried.join(' | ')}`;
}

module.exports = {
  ensureSchema,
  seedIfEmpty,
  getSeries,
  getLatest,
  refreshLatest,
};
