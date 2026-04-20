// ---------------------------------------------------------------------------
// Auto Market Data — long-history monthly reference index
//
// Data origin: industry-standard wholesale used-vehicle value index (monthly).
// We pull the same publisher's free XLSX files and seed a local DB. Nothing
// labelled "Manheim" or "Cox" is exposed to the frontend — all UI surfaces
// say "Auto Market Data". The source URLs are kept here only so the monthly
// refresh cron can pick up new releases.
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
      index_value REAL NOT NULL,
      index_usd_sa REAL,
      seasonal_factor REAL,
      index_usd_nsa REAL,
      mom_pct REAL,
      yoy_pct REAL,
      updated_at TEXT NOT NULL
    )
  `);
  db.exec('CREATE INDEX IF NOT EXISTS idx_amd_date ON auto_market_data_monthly(date)');
}

// ---------- Seed from shipped JSON on first run ----------
function seedIfEmpty(db) {
  const count = db.prepare('SELECT COUNT(*) AS c FROM auto_market_data_monthly').get().c;
  if (count > 0) return { seeded: false, existing: count };

  if (!fs.existsSync(SEED_FILE)) {
    console.warn(`[auto-market-data] seed file missing: ${SEED_FILE}`);
    return { seeded: false, existing: 0 };
  }

  const seed = JSON.parse(fs.readFileSync(SEED_FILE, 'utf8'));
  const now = new Date().toISOString();
  const ins = db.prepare(`
    INSERT OR REPLACE INTO auto_market_data_monthly
      (date, index_value, index_usd_sa, seasonal_factor, index_usd_nsa, mom_pct, yoy_pct, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const tx = db.transaction(rows => {
    for (const r of rows) {
      const date = String(r.date).slice(0, 10); // YYYY-MM-DD
      const idx  = numOrNull(r['Index (1/97 = 100)']);
      if (idx == null) continue;
      ins.run(
        date,
        idx,
        numOrNull(r['Manheim Index $ amount SA']),
        numOrNull(r['Seasonal adjustment factor']),
        numOrNull(r['Manheim Index $ amount NSA']),
        numOrNull(r['SA Price % MoM'] ?? r['Index % MoM']),
        numOrNull(r['Index % YoY'] ?? r['NSA Price % YoY']),
        now,
      );
    }
  });
  tx(seed);
  const finalCount = db.prepare('SELECT COUNT(*) AS c FROM auto_market_data_monthly').get().c;
  console.log(`[auto-market-data] seeded ${finalCount} monthly points`);
  return { seeded: true, existing: finalCount };
}

function numOrNull(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// ---------- Query helper ----------
function getSeries(db, sinceIso) {
  // sinceIso is ISO datetime (e.g. 1900-01-01T00:00:00.000Z). Compare on date portion.
  const sinceDate = sinceIso ? sinceIso.slice(0, 10) : '1900-01-01';
  return db.prepare(`
    SELECT date, index_value
    FROM auto_market_data_monthly
    WHERE date >= ?
    ORDER BY date ASC
  `).all(sinceDate).map(r => ({
    // Use a proper ISO datetime so the frontend time axis works uniformly
    timestamp: r.date + 'T00:00:00.000Z',
    value: r.index_value,
  }));
}

function getLatest(db) {
  return db.prepare(`
    SELECT date, index_value, mom_pct, yoy_pct
    FROM auto_market_data_monthly
    ORDER BY date DESC LIMIT 1
  `).get();
}

// ---------- Monthly refresh (fetch latest XLSX when available) ----------
//
// URL patterns observed:
//   https://www.coxautoinc.com/wp-content/uploads/<pub_YYYY>/<pub_MM>/
//     <MonthName>-<data_YYYY>-Manheim-Used-Vehicle-Value-Index.xlsx   (full name)
//     <Mon>-<data_YYYY>-Manheim-Used-Vehicle-Value-Index.xlsx         (short name)
// Publication of data for month M happens on the 5th business day of M+1.
//
// This function probes candidate URLs for the last 2 months we don't yet have
// and, if an XLSX comes back 200, parses it and upserts rows. It returns a
// short log string for observability. It never blocks startup.

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

function candidateUrls(dataYear, dataMonth /* 1-12 */) {
  // Publication month = dataMonth + 1 (with rollover)
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

async function refreshLatest(db, openpyxlParser) {
  const latest = getLatest(db);
  if (!latest) return 'no existing data; run seed first';
  const [yStr, mStr] = latest.date.split('-');
  let year = Number(yStr), month = Number(mStr); // most-recent in DB
  // Advance to next month and try
  const tried = [];
  for (let step = 1; step <= 2; step++) {
    month += 1; if (month > 12) { month = 1; year += 1; }
    const urls = candidateUrls(year, month);
    for (const url of urls) {
      const r = await httpGetBuffer(url);
      tried.push(`${url} → ${r.ok ? 'OK' : r.status || 'err'}`);
      if (r.ok) {
        // Save, parse via shell (openpyxl in python) — simplest and avoids pulling in a node xlsx lib
        const tmp = path.join(__dirname, 'data', 'seed', `_latest-${year}-${String(month).padStart(2,'0')}.xlsx`);
        fs.writeFileSync(tmp, r.buf);
        try {
          const added = await openpyxlParser(db, tmp);
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
