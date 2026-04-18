const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https');
const { rateLimiter } = require('./rate-limiter');

const app = express();
const PORT = process.env.PORT || 5006;

// Ensure data directory exists
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

// Initialize SQLite
const dbPath = path.join(dataDir, 'auto_markets.db');
const db = new Database(dbPath);

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------
db.exec(`
  CREATE TABLE IF NOT EXISTS auto_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    mean_price REAL,
    median_price REAL,
    listing_count INTEGER
  )
`);
db.exec('CREATE INDEX IF NOT EXISTS idx_auto_prices_timestamp ON auto_prices(timestamp)');
db.exec('CREATE INDEX IF NOT EXISTS idx_auto_prices_make_model ON auto_prices(make, model)');

db.exec(`
  CREATE TABLE IF NOT EXISTS auto_index (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    index_value REAL NOT NULL
  )
`);
db.exec('CREATE INDEX IF NOT EXISTS idx_auto_index_timestamp ON auto_index(timestamp)');

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------
const apiLimiter    = rateLimiter({ windowMs: 60000, max: 60,  message: 'Too many API requests. Please wait a moment.' });
const exportLimiter = rateLimiter({ windowMs: 60000, max: 5,   message: 'Export rate limit exceeded. Please wait before exporting again.' });
const authLimiter   = rateLimiter({ windowMs: 60000, max: 10,  message: 'Too many auth attempts. Please wait.' });

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------
const DASHBOARD_PRODUCT_ID = 'prod_UMBaIV4VS1YzmE'; // Auto Market Index

function validateApiKeyRemote(apiKey) {
  return new Promise((resolve) => {
    const url = `http://localhost:5010/auth/validate-key?key=${encodeURIComponent(apiKey)}&product=${encodeURIComponent(DASHBOARD_PRODUCT_ID)}`;
    require('http').get(url, (resp) => {
      let data = '';
      resp.on('data', chunk => data += chunk);
      resp.on('end', () => {
        try { resolve(JSON.parse(data)); } catch(e) { resolve({ valid: false }); }
      });
    }).on('error', () => resolve({ valid: false }));
  });
}

const apiKeyCache = new Map();

async function requireAuth(req, res, next) {
  const tier = req.headers['x-auth-plan-tier'];
  if (tier) {
    req.planTier = tier;
    return next();
  }

  const apiKey = req.headers['x-api-key'];
  if (apiKey) {
    const cached = apiKeyCache.get(apiKey);
    if (cached && cached.expires > Date.now()) {
      req.planTier = cached.tier;
      return next();
    }

    try {
      const data = await validateApiKeyRemote(apiKey);
      if (data.valid) {
        apiKeyCache.set(apiKey, { tier: data.tier, email: data.email, expires: Date.now() + 60000 });
        req.planTier = data.tier;
        return next();
      }
    } catch(e) { /* auth server unreachable */ }

    return res.status(401).json({ error: 'Invalid API key' });
  }

  return res.status(401).json({ error: 'Authentication required. Access this dashboard through the website.' });
}

function requirePro(req, res, next) {
  if (!req.planTier) {
    return res.status(401).json({ error: 'Authentication required.' });
  }
  if (req.planTier !== 'pro') {
    return res.status(403).json({ error: 'Pro subscription required for API access. Upgrade at https://quantitativegenius.com' });
  }
  next();
}

app.get('/api/user-tier', apiLimiter, requireAuth, (req, res) => {
  res.json({ tier: req.planTier });
});

// ---------------------------------------------------------------------------
// 25 models to track
// ---------------------------------------------------------------------------
const MODELS = [
  // Pickups
  { make: 'Ford',       model: 'F-150',           category: 'Pickup'  },
  { make: 'Chevrolet',  model: 'Silverado 1500',   category: 'Pickup'  },
  { make: 'Toyota',     model: 'Tacoma',            category: 'Pickup'  },
  { make: 'Ram',        model: '1500',              category: 'Pickup'  },
  { make: 'Toyota',     model: 'Tundra',            category: 'Pickup'  },
  // Sedans
  { make: 'Toyota',     model: 'Camry',             category: 'Sedan'   },
  { make: 'Honda',      model: 'Accord',            category: 'Sedan'   },
  { make: 'Toyota',     model: 'Corolla',           category: 'Sedan'   },
  { make: 'Honda',      model: 'Civic',             category: 'Sedan'   },
  { make: 'Nissan',     model: 'Altima',            category: 'Sedan'   },
  { make: 'Hyundai',    model: 'Elantra',           category: 'Sedan'   },
  // SUVs
  { make: 'Toyota',     model: 'RAV4',              category: 'SUV'     },
  { make: 'Honda',      model: 'CR-V',              category: 'SUV'     },
  { make: 'Ford',       model: 'Explorer',          category: 'SUV'     },
  { make: 'Chevrolet',  model: 'Equinox',           category: 'SUV'     },
  { make: 'Toyota',     model: 'Highlander',        category: 'SUV'     },
  { make: 'Jeep',       model: 'Grand Cherokee',    category: 'SUV'     },
  { make: 'Ford',       model: 'Escape',            category: 'SUV'     },
  { make: 'Honda',      model: 'Pilot',             category: 'SUV'     },
  // Luxury
  { make: 'Tesla',      model: 'Model 3',           category: 'Luxury'  },
  { make: 'Tesla',      model: 'Model Y',           category: 'Luxury'  },
  { make: 'BMW',        model: '3 Series',          category: 'Luxury'  },
  { make: 'Mercedes-Benz', model: 'C-Class',        category: 'Luxury'  },
  // Minivan / Commercial
  { make: 'Honda',      model: 'Odyssey',           category: 'Minivan' },
  { make: 'Ford',       model: 'Transit',           category: 'Commercial' },
];

// ---------------------------------------------------------------------------
// Marketcheck API helper
// ---------------------------------------------------------------------------
const MC_API_KEY = '1uyN1GaPrBgPeH084SJ8ihGPQ2wyjxIU';

// Rate limiting: 5 calls/sec max. We poll 25 models with 300ms delay each
// = ~7.5 seconds total per poll cycle (well under 5/sec burst limit).
// Weekly scheduling = 25 calls × 52 weeks = 1,300/year, ~25/week.
// Free tier = 500/month → we stay well under at ~100/month.

function fetchMarketcheck(make, model) {
  return new Promise((resolve, reject) => {
    const makeEnc  = encodeURIComponent(make);
    const modelEnc = encodeURIComponent(model);
    const url = `https://mc-api.marketcheck.com/v2/search/car/active?api_key=${MC_API_KEY}&make=${makeEnc}&model=${modelEnc}&car_type=used&stats=price&rows=0`;

    const options = {
      headers: { 'User-Agent': 'QuantitativeGenius/1.0' },
      timeout: 20000,
    };

    https.get(url, options, (resp) => {
      let body = '';
      resp.on('data', chunk => body += chunk);
      resp.on('end', () => {
        try {
          const json = JSON.parse(body);
          // Marketcheck stats response shape:
          // { stats: { price: { mean: ..., median: ..., count: ... } } }
          const stats = json.stats && json.stats.price;
          if (!stats) return reject(new Error(`No stats returned for ${make} ${model}: ${body.slice(0, 200)}`));
          resolve({
            mean_price:    stats.mean   || null,
            median_price:  stats.median || null,
            listing_count: stats.count  || 0,
          });
        } catch(e) { reject(new Error(`Parse error for ${make} ${model}: ${e.message}`)); }
      });
    }).on('timeout', () => reject(new Error(`Timeout for ${make} ${model}`))).on('error', reject);
  });
}

// ---------------------------------------------------------------------------
// Poll all 25 models sequentially (respects 5 calls/sec limit with delay)
// ---------------------------------------------------------------------------
async function pollAllModels() {
  console.log(`[${new Date().toISOString()}] Starting weekly auto market poll (${MODELS.length} models)...`);
  const timestamp = new Date().toISOString();
  const medians = [];

  for (const m of MODELS) {
    try {
      const stats = await fetchMarketcheck(m.make, m.model);
      db.prepare(`
        INSERT INTO auto_prices (timestamp, make, model, mean_price, median_price, listing_count)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(timestamp, m.make, m.model, stats.mean_price, stats.median_price, stats.listing_count);

      console.log(`  ${m.make} ${m.model}: median=$${stats.median_price?.toLocaleString()}, count=${stats.listing_count}`);
      if (stats.median_price && stats.median_price > 0) {
        medians.push(stats.median_price);
      }
    } catch(err) {
      console.error(`  ERROR ${m.make} ${m.model}:`, err.message);
    }

    // 300ms delay between calls = max ~3.3 calls/sec (under the 5/sec limit)
    await new Promise(r => setTimeout(r, 300));
  }

  // Compute AUTO INDEX = average of all available medians
  if (medians.length > 0) {
    const indexValue = medians.reduce((a, b) => a + b, 0) / medians.length;
    db.prepare('INSERT INTO auto_index (timestamp, index_value) VALUES (?, ?)').run(timestamp, Math.round(indexValue * 100) / 100);
    console.log(`[${new Date().toISOString()}] AUTO INDEX: $${Math.round(indexValue).toLocaleString()} (from ${medians.length} models)`);
  } else {
    console.warn('[poll] No medians collected — AUTO INDEX not stored');
  }
}

// ---------------------------------------------------------------------------
// Helper: time range since-date (weekly granularity)
// ---------------------------------------------------------------------------
function getSince(range) {
  const now = new Date();
  switch (range) {
    case '1W':  return new Date(now - 7 * 86400000).toISOString();
    case '1M':  return new Date(now - 30 * 86400000).toISOString();
    case '1Y':  return new Date(now - 365 * 86400000).toISOString();
    case 'MAX': return '1900-01-01T00:00:00.000Z';
    default:    return '1900-01-01T00:00:00.000Z';
  }
}

// ---------------------------------------------------------------------------
// GET /api/current — latest AUTO INDEX + all 25 model prices
// ---------------------------------------------------------------------------
app.get('/api/current', apiLimiter, requireAuth, (req, res) => {
  try {
    const latestIndex  = db.prepare('SELECT * FROM auto_index ORDER BY timestamp DESC LIMIT 1').get();
    const previousIndex = db.prepare('SELECT * FROM auto_index ORDER BY timestamp DESC LIMIT 1 OFFSET 1').get();

    // Latest price for each model
    const modelPrices = MODELS.map(m => {
      const latest = db.prepare(`
        SELECT * FROM auto_prices
        WHERE make = ? AND model = ?
        ORDER BY timestamp DESC LIMIT 1
      `).get(m.make, m.model);

      const previous = db.prepare(`
        SELECT * FROM auto_prices
        WHERE make = ? AND model = ?
        ORDER BY timestamp DESC LIMIT 1 OFFSET 1
      `).get(m.make, m.model);

      let changePercent = null;
      if (latest && previous && previous.median_price) {
        changePercent = ((latest.median_price - previous.median_price) / previous.median_price) * 100;
        changePercent = Math.round(changePercent * 100) / 100;
      }

      return {
        make: m.make,
        model: m.model,
        category: m.category,
        median_price: latest ? latest.median_price : null,
        mean_price: latest ? latest.mean_price : null,
        listing_count: latest ? latest.listing_count : null,
        change_percent: changePercent,
        timestamp: latest ? latest.timestamp : null,
      };
    });

    let indexChange = null;
    let indexChangePct = null;
    let trend = 'stable';
    if (latestIndex && previousIndex) {
      indexChange    = latestIndex.index_value - previousIndex.index_value;
      indexChangePct = (indexChange / previousIndex.index_value) * 100;
      if (indexChangePct >= 3)       trend = 'surging';
      else if (indexChangePct >= 1)  trend = 'rising';
      else if (indexChangePct <= -3) trend = 'plunging';
      else if (indexChangePct <= -1) trend = 'falling';
      else                           trend = 'stable';
    }

    res.json({
      index_value:      latestIndex ? latestIndex.index_value : null,
      index_timestamp:  latestIndex ? latestIndex.timestamp : null,
      index_change:     indexChange  ? Math.round(indexChange * 100) / 100 : null,
      index_change_pct: indexChangePct ? Math.round(indexChangePct * 100) / 100 : null,
      trend,
      models: modelPrices,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /api/index-history?range=1W|1M|1Y|MAX — AUTO INDEX chart data
// ---------------------------------------------------------------------------
app.get('/api/index-history', apiLimiter, requireAuth, (req, res) => {
  try {
    const range = req.query.range || 'MAX';
    const since = getSince(range);

    const readings = db.prepare(`
      SELECT timestamp, index_value as value
      FROM auto_index
      WHERE timestamp >= ?
      ORDER BY timestamp ASC
    `).all(since);

    res.json({ readings });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// GET /api/model-history?make=Ford&model=F-150&range=MAX — per-model chart
// ---------------------------------------------------------------------------
app.get('/api/model-history', apiLimiter, requireAuth, (req, res) => {
  try {
    const { make, model, range } = req.query;
    if (!make || !model) return res.status(400).json({ error: 'make and model required' });
    const since = getSince(range || 'MAX');

    const readings = db.prepare(`
      SELECT timestamp, median_price as value, mean_price, listing_count
      FROM auto_prices
      WHERE make = ? AND model = ? AND timestamp >= ?
      ORDER BY timestamp ASC
    `).all(make, model, since);

    res.json({ readings });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// Pro-only export endpoints
// ---------------------------------------------------------------------------
const EXPORT_DIR = path.join(__dirname, 'data', 'exports');
const DAILY_DIR  = path.join(EXPORT_DIR, 'daily');
if (!fs.existsSync(EXPORT_DIR)) fs.mkdirSync(EXPORT_DIR, { recursive: true });
if (!fs.existsSync(DAILY_DIR))  fs.mkdirSync(DAILY_DIR,  { recursive: true });

function tryServeFile(filePath, contentType, downloadName, res) {
  if (fs.existsSync(filePath)) {
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `attachment; filename="${downloadName}"`);
    res.setHeader('X-Export-Source', 'pre-generated');
    return res.sendFile(filePath);
  }
  return false;
}

// --- CSV export ---
app.get('/api/export/csv', exportLimiter, requireAuth, requirePro, (req, res) => {
  try {
    const range = req.query.range || 'MAX';

    if (range === 'MAX') {
      const file = path.join(EXPORT_DIR, 'auto-markets-history.csv');
      if (tryServeFile(file, 'text/csv', 'auto-market-index-MAX.csv', res)) return;
    }

    const since = getSince(range);

    // Get all index readings
    const indexRows = db.prepare(`
      SELECT date(timestamp) as date, index_value
      FROM auto_index WHERE timestamp >= ?
      ORDER BY date ASC
    `).all(since);

    // Build model price maps per date
    const modelMaps = {};
    for (const m of MODELS) {
      const key = `${m.make}|${m.model}`;
      modelMaps[key] = {};
      const rows = db.prepare(`
        SELECT date(timestamp) as date, median_price
        FROM auto_prices WHERE make = ? AND model = ? AND timestamp >= ?
        ORDER BY date ASC
      `).all(m.make, m.model, since);
      for (const r of rows) modelMaps[key][r.date] = r.median_price;
    }

    // Build CSV header
    const modelCols = MODELS.map(m => `${m.make.replace(/[,\s]/g,'_')}_${m.model.replace(/[,\s]/g,'_')}_median`);
    let csv = ['date', 'auto_index', ...modelCols].join(',') + '\n';

    for (const row of indexRows) {
      const vals = MODELS.map(m => {
        const v = modelMaps[`${m.make}|${m.model}`][row.date];
        return v !== undefined ? v : '';
      });
      csv += [row.date, row.index_value, ...vals].join(',') + '\n';
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="auto-market-index-${range}.csv"`);
    res.setHeader('X-Export-Source', 'live-query');
    res.send(csv);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- JSON export ---
app.get('/api/export/json', exportLimiter, requireAuth, requirePro, (req, res) => {
  try {
    const range = req.query.range || 'MAX';

    if (range === 'MAX') {
      const file = path.join(EXPORT_DIR, 'auto-markets-history.json');
      if (tryServeFile(file, 'application/json', 'auto-market-index-MAX.json', res)) return;
    }

    const since = getSince(range);

    const indexRows = db.prepare(`
      SELECT date(timestamp) as date, index_value
      FROM auto_index WHERE timestamp >= ?
      ORDER BY date ASC
    `).all(since);

    const data = indexRows.map(row => {
      const entry = { date: row.date, auto_index: row.index_value, models: {} };
      for (const m of MODELS) {
        const price = db.prepare(`
          SELECT median_price FROM auto_prices
          WHERE make = ? AND model = ? AND date(timestamp) = ?
          ORDER BY timestamp DESC LIMIT 1
        `).get(m.make, m.model, row.date);
        entry.models[`${m.make} ${m.model}`] = price ? price.median_price : null;
      }
      return entry;
    });

    res.setHeader('Content-Disposition', `attachment; filename="auto-market-index-${range}.json"`);
    res.setHeader('X-Export-Source', 'live-query');
    res.json({ export_date: new Date().toISOString(), range, record_count: data.length, data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// Auth proxy: forward /api/auth/* to auth server at localhost:5010
// ---------------------------------------------------------------------------
function proxyToAuth(method) {
  return (req, res) => {
    const authPath = req.path.replace('/api/auth', '/auth');
    const options = {
      hostname: 'localhost',
      port: 5010,
      path: authPath,
      method: method,
      headers: { cookie: req.headers.cookie || '' },
    };
    const proxyReq = require('http').request(options, (proxyRes) => {
      let data = '';
      proxyRes.on('data', chunk => data += chunk);
      proxyRes.on('end', () => {
        res.status(proxyRes.statusCode);
        if (proxyRes.headers['set-cookie']) res.setHeader('set-cookie', proxyRes.headers['set-cookie']);
        res.setHeader('Content-Type', 'application/json');
        res.send(data);
      });
    });
    proxyReq.on('error', () => res.status(502).json({ error: 'Auth server unreachable' }));
    proxyReq.end();
  };
}

app.get('/api/auth/api-key-status', authLimiter, proxyToAuth('GET'));
app.post('/api/auth/api-key', authLimiter, proxyToAuth('POST'));
app.delete('/api/auth/api-key', authLimiter, proxyToAuth('DELETE'));

// ---------------------------------------------------------------------------
// Scheduling: weekly poll (every 7 days)
// Startup: poll immediately if auto_prices table is empty
// ---------------------------------------------------------------------------
const WEEKLY_MS = 7 * 24 * 3600 * 1000;

// Startup poll if DB is empty
setTimeout(async () => {
  const count = db.prepare('SELECT COUNT(*) as cnt FROM auto_prices').get().cnt;
  if (count === 0) {
    console.log('[startup] auto_prices table empty — running initial poll...');
    await pollAllModels();
  } else {
    console.log(`[startup] auto_prices has ${count} rows — skipping initial poll`);
  }
}, 5000);

// Weekly recurring poll
setInterval(pollAllModels, WEEKLY_MS);

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
  console.log(`Auto Market Index Dashboard running on http://localhost:${PORT}`);
  console.log(`Tracking ${MODELS.length} models | Weekly polling | Port ${PORT}`);
});
