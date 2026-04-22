const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const https = require('https');
const { rateLimiter } = require('./rate-limiter');
const { spawn } = require('child_process');
const amd = require('./auto-market-data');

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

// Auto Market Data: long-history monthly reference series
amd.ensureSchema(db);
amd.seedIfEmpty(db);

// MostLiked index — quarterly 7-segment + Overall (v2 methodology)
db.exec(`
  CREATE TABLE IF NOT EXISTS mostliked_index (
    timestamp TEXT NOT NULL,
    quarter TEXT NOT NULL,
    segment TEXT NOT NULL,
    label TEXT NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (quarter, segment)
  )
`);
db.exec('CREATE INDEX IF NOT EXISTS idx_mostliked_timestamp ON mostliked_index(timestamp)');
const mostlikedSeedPath = path.join(__dirname, 'data', 'seed', 'mostliked_v2_seed.json');
const mostlikedCount = db.prepare('SELECT COUNT(*) AS n FROM mostliked_index').get().n;
if (mostlikedCount === 0 && fs.existsSync(mostlikedSeedPath)) {
  const seed = JSON.parse(fs.readFileSync(mostlikedSeedPath, 'utf8'));
  const ins = db.prepare('INSERT OR REPLACE INTO mostliked_index (timestamp, quarter, segment, label, value) VALUES (?, ?, ?, ?, ?)');
  const tx = db.transaction((rows) => { for (const r of rows) ins.run(r.timestamp, r.quarter, r.segment, r.label, r.value); });
  tx(seed.rows);
  console.log(`Seeded mostliked_index with ${seed.rows.length} rows`);
}

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
// 185 models across 10 segments (Auto_Market_Index_Basket_v1.0)
// Segment slugs match methodology v2.1 + basket doc:
// entry_suv, midsize_suv, fullsize_suv, luxury_suv,
// compact_car, midsize_car, luxury_car, sports_car, pickup, van
// ---------------------------------------------------------------------------
const MODELS = [
  // Entry SUV (25)
  { make: 'Honda', model: 'CR-V', segment: 'entry_suv' },
  { make: 'Toyota', model: 'RAV4', segment: 'entry_suv' },
  { make: 'Ford', model: 'Escape', segment: 'entry_suv' },
  { make: 'Chevrolet', model: 'Equinox', segment: 'entry_suv' },
  { make: 'Nissan', model: 'Rogue', segment: 'entry_suv' },
  { make: 'Kia', model: 'Sportage', segment: 'entry_suv' },
  { make: 'Hyundai', model: 'Tucson', segment: 'entry_suv' },
  { make: 'Subaru', model: 'Forester', segment: 'entry_suv' },
  { make: 'Mazda', model: 'CX-5', segment: 'entry_suv' },
  { make: 'Jeep', model: 'Compass', segment: 'entry_suv' },
  { make: 'Jeep', model: 'Cherokee', segment: 'entry_suv' },
  { make: 'Honda', model: 'HR-V', segment: 'entry_suv' },
  { make: 'Toyota', model: 'Corolla Cross', segment: 'entry_suv' },
  { make: 'Hyundai', model: 'Kona', segment: 'entry_suv' },
  { make: 'Kia', model: 'Seltos', segment: 'entry_suv' },
  { make: 'Subaru', model: 'Crosstrek', segment: 'entry_suv' },
  { make: 'Mazda', model: 'CX-30', segment: 'entry_suv' },
  { make: 'Nissan', model: 'Kicks', segment: 'entry_suv' },
  { make: 'Buick', model: 'Encore', segment: 'entry_suv' },
  { make: 'Chevrolet', model: 'Trax', segment: 'entry_suv' },
  { make: 'Ford', model: 'Bronco Sport', segment: 'entry_suv' },
  { make: 'Jeep', model: 'Renegade', segment: 'entry_suv' },
  { make: 'Volkswagen', model: 'Taos', segment: 'entry_suv' },
  { make: 'Mitsubishi', model: 'Outlander Sport', segment: 'entry_suv' },
  { make: 'Tesla', model: 'Model Y', segment: 'entry_suv' },
  // Midsize SUV (25)
  { make: 'Toyota', model: 'Highlander', segment: 'midsize_suv' },
  { make: 'Honda', model: 'Pilot', segment: 'midsize_suv' },
  { make: 'Jeep', model: 'Grand Cherokee', segment: 'midsize_suv' },
  { make: 'Ford', model: 'Explorer', segment: 'midsize_suv' },
  { make: 'Chevrolet', model: 'Traverse', segment: 'midsize_suv' },
  { make: 'Nissan', model: 'Pathfinder', segment: 'midsize_suv' },
  { make: 'Kia', model: 'Sorento', segment: 'midsize_suv' },
  { make: 'Kia', model: 'Telluride', segment: 'midsize_suv' },
  { make: 'Hyundai', model: 'Palisade', segment: 'midsize_suv' },
  { make: 'Hyundai', model: 'Santa Fe', segment: 'midsize_suv' },
  { make: 'Nissan', model: 'Murano', segment: 'midsize_suv' },
  { make: 'Honda', model: 'Passport', segment: 'midsize_suv' },
  { make: 'Volkswagen', model: 'Atlas', segment: 'midsize_suv' },
  { make: 'Mazda', model: 'CX-9', segment: 'midsize_suv' },
  { make: 'Mazda', model: 'CX-90', segment: 'midsize_suv' },
  { make: 'Subaru', model: 'Ascent', segment: 'midsize_suv' },
  { make: 'Ford', model: 'Edge', segment: 'midsize_suv' },
  { make: 'Chevrolet', model: 'Blazer', segment: 'midsize_suv' },
  { make: 'GMC', model: 'Acadia', segment: 'midsize_suv' },
  { make: 'Buick', model: 'Enclave', segment: 'midsize_suv' },
  { make: 'Dodge', model: 'Durango', segment: 'midsize_suv' },
  { make: 'Toyota', model: '4Runner', segment: 'midsize_suv' },
  { make: 'Jeep', model: 'Wrangler', segment: 'midsize_suv' },
  { make: 'Jeep', model: 'Wrangler 4xe', segment: 'midsize_suv' },
  { make: 'Toyota', model: 'Grand Highlander', segment: 'midsize_suv' },
  // Fullsize SUV (15)
  { make: 'Chevrolet', model: 'Tahoe', segment: 'fullsize_suv' },
  { make: 'Chevrolet', model: 'Suburban', segment: 'fullsize_suv' },
  { make: 'Ford', model: 'Expedition', segment: 'fullsize_suv' },
  { make: 'Ford', model: 'Expedition MAX', segment: 'fullsize_suv' },
  { make: 'GMC', model: 'Yukon', segment: 'fullsize_suv' },
  { make: 'GMC', model: 'Yukon XL', segment: 'fullsize_suv' },
  { make: 'Toyota', model: 'Sequoia', segment: 'fullsize_suv' },
  { make: 'Nissan', model: 'Armada', segment: 'fullsize_suv' },
  { make: 'Jeep', model: 'Wagoneer', segment: 'fullsize_suv' },
  { make: 'Jeep', model: 'Grand Wagoneer', segment: 'fullsize_suv' },
  { make: 'Lincoln', model: 'Navigator', segment: 'fullsize_suv' },
  { make: 'Cadillac', model: 'Escalade', segment: 'fullsize_suv' },
  { make: 'Cadillac', model: 'Escalade ESV', segment: 'fullsize_suv' },
  { make: 'Infiniti', model: 'QX80', segment: 'fullsize_suv' },
  { make: 'Lexus', model: 'LX', segment: 'fullsize_suv' },
  // Luxury SUV (20)
  { make: 'BMW', model: 'X5', segment: 'luxury_suv' },
  { make: 'BMW', model: 'X3', segment: 'luxury_suv' },
  { make: 'BMW', model: 'X7', segment: 'luxury_suv' },
  { make: 'Mercedes-Benz', model: 'GLE', segment: 'luxury_suv' },
  { make: 'Mercedes-Benz', model: 'GLC', segment: 'luxury_suv' },
  { make: 'Mercedes-Benz', model: 'GLS', segment: 'luxury_suv' },
  { make: 'Lexus', model: 'RX', segment: 'luxury_suv' },
  { make: 'Lexus', model: 'NX', segment: 'luxury_suv' },
  { make: 'Lexus', model: 'GX', segment: 'luxury_suv' },
  { make: 'Audi', model: 'Q5', segment: 'luxury_suv' },
  { make: 'Audi', model: 'Q7', segment: 'luxury_suv' },
  { make: 'Audi', model: 'Q8', segment: 'luxury_suv' },
  { make: 'Acura', model: 'MDX', segment: 'luxury_suv' },
  { make: 'Acura', model: 'RDX', segment: 'luxury_suv' },
  { make: 'Volvo', model: 'XC90', segment: 'luxury_suv' },
  { make: 'Volvo', model: 'XC60', segment: 'luxury_suv' },
  { make: 'Porsche', model: 'Cayenne', segment: 'luxury_suv' },
  { make: 'Porsche', model: 'Macan', segment: 'luxury_suv' },
  { make: 'Range Rover', model: 'Sport', segment: 'luxury_suv' },
  { make: 'Genesis', model: 'GV70', segment: 'luxury_suv' },
  // Compact Car (15)
  { make: 'Honda', model: 'Civic', segment: 'compact_car' },
  { make: 'Toyota', model: 'Corolla', segment: 'compact_car' },
  { make: 'Hyundai', model: 'Elantra', segment: 'compact_car' },
  { make: 'Nissan', model: 'Sentra', segment: 'compact_car' },
  { make: 'Mazda', model: 'Mazda3', segment: 'compact_car' },
  { make: 'Kia', model: 'Forte', segment: 'compact_car' },
  { make: 'Volkswagen', model: 'Jetta', segment: 'compact_car' },
  { make: 'Subaru', model: 'Impreza', segment: 'compact_car' },
  { make: 'Chevrolet', model: 'Trailblazer', segment: 'compact_car' },
  { make: 'Honda', model: 'Fit', segment: 'compact_car' },
  { make: 'Toyota', model: 'Prius', segment: 'compact_car' },
  { make: 'Kia', model: 'Rio', segment: 'compact_car' },
  { make: 'Hyundai', model: 'Accent', segment: 'compact_car' },
  { make: 'Nissan', model: 'Versa', segment: 'compact_car' },
  { make: 'Mitsubishi', model: 'Mirage', segment: 'compact_car' },
  // Midsize Car (20)
  { make: 'Toyota', model: 'Camry', segment: 'midsize_car' },
  { make: 'Honda', model: 'Accord', segment: 'midsize_car' },
  { make: 'Nissan', model: 'Altima', segment: 'midsize_car' },
  { make: 'Hyundai', model: 'Sonata', segment: 'midsize_car' },
  { make: 'Kia', model: 'K5', segment: 'midsize_car' },
  { make: 'Chevrolet', model: 'Malibu', segment: 'midsize_car' },
  { make: 'Subaru', model: 'Legacy', segment: 'midsize_car' },
  { make: 'Mazda', model: 'Mazda6', segment: 'midsize_car' },
  { make: 'Tesla', model: 'Model 3', segment: 'midsize_car' },
  { make: 'Volkswagen', model: 'Passat', segment: 'midsize_car' },
  { make: 'Volkswagen', model: 'Arteon', segment: 'midsize_car' },
  { make: 'Toyota', model: 'Avalon', segment: 'midsize_car' },
  { make: 'Chrysler', model: '300', segment: 'midsize_car' },
  { make: 'Dodge', model: 'Charger', segment: 'midsize_car' },
  { make: 'Nissan', model: 'Maxima', segment: 'midsize_car' },
  { make: 'Hyundai', model: 'Elantra GT', segment: 'midsize_car' },
  { make: 'Subaru', model: 'Outback', segment: 'midsize_car' },
  { make: 'Buick', model: 'Regal', segment: 'midsize_car' },
  { make: 'Ford', model: 'Fusion', segment: 'midsize_car' },
  { make: 'Chevrolet', model: 'Cruze', segment: 'midsize_car' },
  // Luxury Car (15)
  { make: 'BMW', model: '3 Series', segment: 'luxury_car' },
  { make: 'BMW', model: '5 Series', segment: 'luxury_car' },
  { make: 'BMW', model: '7 Series', segment: 'luxury_car' },
  { make: 'Mercedes-Benz', model: 'C-Class', segment: 'luxury_car' },
  { make: 'Mercedes-Benz', model: 'E-Class', segment: 'luxury_car' },
  { make: 'Mercedes-Benz', model: 'S-Class', segment: 'luxury_car' },
  { make: 'Audi', model: 'A4', segment: 'luxury_car' },
  { make: 'Audi', model: 'A6', segment: 'luxury_car' },
  { make: 'Audi', model: 'A8', segment: 'luxury_car' },
  { make: 'Lexus', model: 'ES', segment: 'luxury_car' },
  { make: 'Lexus', model: 'IS', segment: 'luxury_car' },
  { make: 'Lexus', model: 'LS', segment: 'luxury_car' },
  { make: 'Acura', model: 'TLX', segment: 'luxury_car' },
  { make: 'Infiniti', model: 'Q50', segment: 'luxury_car' },
  { make: 'Genesis', model: 'G70', segment: 'luxury_car' },
  // Sports Car (10)
  { make: 'Ford', model: 'Mustang', segment: 'sports_car' },
  { make: 'Chevrolet', model: 'Camaro', segment: 'sports_car' },
  { make: 'Dodge', model: 'Challenger', segment: 'sports_car' },
  { make: 'Chevrolet', model: 'Corvette', segment: 'sports_car' },
  { make: 'BMW', model: 'Z4', segment: 'sports_car' },
  { make: 'Mazda', model: 'MX-5 Miata', segment: 'sports_car' },
  { make: 'Porsche', model: '911', segment: 'sports_car' },
  { make: 'Porsche', model: 'Cayman', segment: 'sports_car' },
  { make: 'Subaru', model: 'BRZ', segment: 'sports_car' },
  { make: 'Toyota', model: 'GR Supra', segment: 'sports_car' },
  // Pickup (30)
  { make: 'Ford', model: 'F-150', segment: 'pickup' },
  { make: 'Ford', model: 'F-250', segment: 'pickup' },
  { make: 'Ford', model: 'F-350', segment: 'pickup' },
  { make: 'Ford', model: 'Ranger', segment: 'pickup' },
  { make: 'Ford', model: 'Maverick', segment: 'pickup' },
  { make: 'Chevrolet', model: 'Silverado 1500', segment: 'pickup' },
  { make: 'Chevrolet', model: 'Silverado 2500', segment: 'pickup' },
  { make: 'Chevrolet', model: 'Silverado 3500', segment: 'pickup' },
  { make: 'Chevrolet', model: 'Colorado', segment: 'pickup' },
  { make: 'GMC', model: 'Sierra 1500', segment: 'pickup' },
  { make: 'GMC', model: 'Sierra 2500', segment: 'pickup' },
  { make: 'GMC', model: 'Canyon', segment: 'pickup' },
  { make: 'Ram', model: '1500', segment: 'pickup' },
  { make: 'Ram', model: '2500', segment: 'pickup' },
  { make: 'Ram', model: '3500', segment: 'pickup' },
  { make: 'Toyota', model: 'Tacoma', segment: 'pickup' },
  { make: 'Toyota', model: 'Tundra', segment: 'pickup' },
  { make: 'Nissan', model: 'Titan', segment: 'pickup' },
  { make: 'Nissan', model: 'Frontier', segment: 'pickup' },
  { make: 'Honda', model: 'Ridgeline', segment: 'pickup' },
  { make: 'Jeep', model: 'Gladiator', segment: 'pickup' },
  { make: 'Hyundai', model: 'Santa Cruz', segment: 'pickup' },
  { make: 'Ford', model: 'F-150 Lightning', segment: 'pickup' },
  { make: 'Rivian', model: 'R1T', segment: 'pickup' },
  { make: 'Tesla', model: 'Cybertruck', segment: 'pickup' },
  { make: 'Chevrolet', model: 'Silverado EV', segment: 'pickup' },
  { make: 'GMC', model: 'Hummer EV', segment: 'pickup' },
  { make: 'Ford', model: 'F-150 Raptor', segment: 'pickup' },
  { make: 'Ram', model: '1500 TRX', segment: 'pickup' },
  { make: 'Toyota', model: 'Tundra TRD', segment: 'pickup' },
  // Van (10)
  { make: 'Honda', model: 'Odyssey', segment: 'van' },
  { make: 'Toyota', model: 'Sienna', segment: 'van' },
  { make: 'Chrysler', model: 'Pacifica', segment: 'van' },
  { make: 'Kia', model: 'Carnival', segment: 'van' },
  { make: 'Chrysler', model: 'Grand Caravan', segment: 'van' },
  { make: 'Dodge', model: 'Grand Caravan', segment: 'van' },
  { make: 'Ford', model: 'Transit', segment: 'van' },
  { make: 'Ford', model: 'Transit Connect', segment: 'van' },
  { make: 'Mercedes-Benz', model: 'Metris', segment: 'van' },
  { make: 'Nissan', model: 'NV200', segment: 'van' },
];

// ---------------------------------------------------------------------------
// Segment configuration (methodology v2.1 §5.1 + §5.3)
// Composite weights renormalized to sum to 1.0 (Sports Car excluded from
// composite; included only in Car Total via CAR_SUBWEIGHTS).
// ---------------------------------------------------------------------------
const SEGMENT_LABELS = {
  entry_suv:    'Entry SUV',
  midsize_suv:  'Midsize SUV',
  fullsize_suv: 'Fullsize SUV',
  luxury_suv:   'Luxury SUV',
  compact_car:  'Compact Car',
  midsize_car:  'Midsize Car',
  luxury_car:   'Luxury Car',
  sports_car:   'Sports Car',
  pickup:       'Pickup',
  van:          'Van',
};

// SUV sub-segment → SUV Total weights (sum to 1.0)
const SUV_SUBWEIGHTS = { entry_suv: 0.40, midsize_suv: 0.35, fullsize_suv: 0.15, luxury_suv: 0.10 };
// Car sub-segment → Car Total weights (sum to 1.0)
const CAR_SUBWEIGHTS = { compact_car: 0.40, midsize_car: 0.45, luxury_car: 0.10, sports_car: 0.05 };
// Composite AMI weights (renormalized; sum to 1.0; sports excluded from composite)
const AMI_COMPOSITE_WEIGHTS = {
  suv_total: 0.50 / 0.95, // 0.5263
  car_total: 0.25 / 0.95, // 0.2632
  pickup:    0.16 / 0.95, // 0.1684
  van:       0.04 / 0.95, // 0.0421
};

function median(arr) {
  const vals = arr.filter(v => Number.isFinite(v) && v > 0).sort((a, b) => a - b);
  if (vals.length === 0) return null;
  const mid = Math.floor(vals.length / 2);
  return vals.length % 2 ? vals[mid] : (vals[mid - 1] + vals[mid]) / 2;
}

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
// Poll all models sequentially (respects 5 calls/sec limit with delay)
// Computes per-segment median, SUV Total, Car Total, and composite AMI.
// ---------------------------------------------------------------------------
async function pollAllModels() {
  console.log(`[${new Date().toISOString()}] Starting monthly auto market poll (${MODELS.length} models)...`);
  const timestamp = new Date().toISOString();

  // Collect per-segment medians from only ACTIVE, non-benched models
  const activeSet = new Set(
    db.prepare('SELECT make, model FROM model_status WHERE is_active = 1 AND is_bench = 0')
      .all().map(r => `${r.make}|${r.model}`)
  );
  const bySegment = {}; // slug -> [median_price, ...]
  for (const slug of Object.keys(SEGMENT_LABELS)) bySegment[slug] = [];

  for (const m of MODELS) {
    if (!activeSet.has(`${m.make}|${m.model}`)) continue; // skip dropped/benched
    try {
      const stats = await fetchMarketcheck(m.make, m.model);
      db.prepare(`
        INSERT INTO auto_prices (timestamp, make, model, mean_price, median_price, listing_count)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(timestamp, m.make, m.model, stats.mean_price, stats.median_price, stats.listing_count);

      console.log(`  [${m.segment}] ${m.make} ${m.model}: median=$${stats.median_price?.toLocaleString()}, count=${stats.listing_count}`);
      if (stats.median_price && stats.median_price > 0) {
        bySegment[m.segment].push(stats.median_price);
      }
    } catch(err) {
      console.error(`  ERROR ${m.make} ${m.model}:`, err.message);
    }

    // 300ms delay between calls = max ~3.3 calls/sec (under the 5/sec limit)
    await new Promise(r => setTimeout(r, 300));
  }

  // Per-segment median (unweighted across basket models in that segment)
  const segMedian = {};
  for (const slug of Object.keys(SEGMENT_LABELS)) {
    segMedian[slug] = median(bySegment[slug]);
  }

  // Sub-totals (weighted combinations of segment medians)
  function weightedSum(parts) {
    let num = 0, w = 0;
    for (const [slug, weight] of Object.entries(parts)) {
      const v = segMedian[slug];
      if (Number.isFinite(v) && v > 0) { num += v * weight; w += weight; }
    }
    return w > 0 ? num / w : null;
  }

  const suvTotal = weightedSum(SUV_SUBWEIGHTS);
  const carTotal = weightedSum(CAR_SUBWEIGHTS);

  // Composite AMI (sports excluded from composite per §5.1)
  const compositeParts = {};
  if (suvTotal != null) compositeParts.suv_total = suvTotal;
  if (carTotal != null) compositeParts.car_total = carTotal;
  if (segMedian.pickup != null) compositeParts.pickup = segMedian.pickup;
  if (segMedian.van != null)    compositeParts.van = segMedian.van;

  let compositeNum = 0, compositeW = 0;
  for (const [key, val] of Object.entries(compositeParts)) {
    const w = AMI_COMPOSITE_WEIGHTS[key];
    compositeNum += val * w;
    compositeW += w;
  }
  const composite = compositeW > 0 ? compositeNum / compositeW : null;

  // Write per-segment rows to auto_segment_history
  const insSeg = db.prepare(`INSERT INTO auto_segment_history
    (timestamp, segment, median_price, model_count, source)
    VALUES (?, ?, ?, ?, 'live')`);
  const segTx = db.transaction(() => {
    for (const slug of Object.keys(SEGMENT_LABELS)) {
      insSeg.run(timestamp, slug, segMedian[slug], bySegment[slug].length);
    }
    // Sub-totals and composite stored as pseudo-segments for unified querying
    if (suvTotal != null)  insSeg.run(timestamp, 'suv_total', Math.round(suvTotal * 100) / 100, null);
    if (carTotal != null)  insSeg.run(timestamp, 'car_total', Math.round(carTotal * 100) / 100, null);
    if (composite != null) insSeg.run(timestamp, 'ami_composite', Math.round(composite * 100) / 100, null);
  });
  segTx();

  // Legacy auto_index (keep writing for backwards compat)
  if (composite != null) {
    db.prepare('INSERT INTO auto_index (timestamp, index_value) VALUES (?, ?)')
      .run(timestamp, Math.round(composite * 100) / 100);
    console.log(`[${new Date().toISOString()}] AMI composite: $${Math.round(composite).toLocaleString()} | SUV=$${Math.round(suvTotal||0).toLocaleString()} Car=$${Math.round(carTotal||0).toLocaleString()} Pickup=$${Math.round(segMedian.pickup||0).toLocaleString()} Van=$${Math.round(segMedian.van||0).toLocaleString()}`);
  } else {
    console.warn('[poll] No composite produced — insufficient segment data');
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
        segment: m.segment,
        segment_label: SEGMENT_LABELS[m.segment] || m.segment,
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
// GET /api/segment-history?range=1M|3M|1Y|5Y|MAX[&segment=<slug>]
// Returns per-segment median series. With ?segment=, returns a single series.
// Without, returns all segments (10 real + suv_total + car_total + ami_composite).
// Stitches live + Manheim-backfill from auto_segment_history, applying
// splice_factor to backfill rows so the two series are continuous at the
// AMD→AMI cutover (methodology v2.1 §9.2).
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET /api/mostliked — quarterly MostLiked index, 7 segments + Overall (8 lines)
// ---------------------------------------------------------------------------
app.get('/api/mostliked', apiLimiter, requireAuth, (req, res) => {
  try {
    const rows = db.prepare('SELECT timestamp, quarter, segment, label, value FROM mostliked_index ORDER BY segment, timestamp').all();
    const lines = {};
    for (const r of rows) {
      if (!lines[r.segment]) lines[r.segment] = { segment: r.segment, label: r.label, readings: [] };
      lines[r.segment].readings.push({ timestamp: r.timestamp, quarter: r.quarter, value: r.value });
    }
    res.json({ lines: Object.values(lines) });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/segment-history', apiLimiter, requireAuth, (req, res) => {
  try {
    const range = req.query.range || 'MAX';
    const since = getSince(range);
    const seg = req.query.segment || null;

    // Load splice factors once (segment -> factor)
    const splices = {};
    for (const r of db.prepare('SELECT segment, splice_factor FROM segment_splice_factors').all()) {
      splices[r.segment] = r.splice_factor;
    }

    const params = [since];
    let sql = `SELECT timestamp, segment, median_price, model_count, source
               FROM auto_segment_history WHERE timestamp >= ?`;
    if (seg) { sql += ' AND segment = ?'; params.push(seg); }
    sql += ' ORDER BY timestamp ASC';

    const rows = db.prepare(sql).all(...params);

    // Apply splice to backfill rows
    const bySegment = {};
    for (const r of rows) {
      const k = r.segment;
      if (!bySegment[k]) bySegment[k] = [];
      let value = r.median_price;
      if (r.source === 'manheim' && splices[k] != null && value != null) {
        value = Math.round(value * splices[k] * 100) / 100;
      }
      bySegment[k].push({
        timestamp: r.timestamp,
        value,
        model_count: r.model_count,
        source: r.source,
      });
    }

    if (seg) {
      res.json({ segment: seg, label: SEGMENT_LABELS[seg] || seg, readings: bySegment[seg] || [] });
    } else {
      res.json({
        segments: Object.keys(bySegment).map(slug => ({
          segment: slug,
          label: SEGMENT_LABELS[slug] || slug,
          readings: bySegment[slug],
        })),
      });
    }
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
// Overlay endpoints: SP500, Gold, BTC (read from stocks dashboard DB)
// ---------------------------------------------------------------------------
const OVERLAY_DB = process.env.OVERLAY_DB || '/home/support/stock-market-time-machine-dashboard/data/stock_markets.db';
const OVERLAY_TABLES = { spx: 'sp500_data', gold: 'gold_data', btc: 'btc_data' };
function openOverlay() {
  try {
    if (!fs.existsSync(OVERLAY_DB) || fs.statSync(OVERLAY_DB).size === 0) return null;
    return new Database(OVERLAY_DB, { readonly: true, fileMustExist: true });
  } catch (e) { return null; }
}
function overlayHandler(which) {
  return (req, res) => {
    try {
      const since = getSince(req.query.range || 'MAX');
      const odb = openOverlay();
      if (!odb) return res.json({ readings: [] });
      const table = OVERLAY_TABLES[which];
      const rows = odb.prepare(`SELECT timestamp, price AS value FROM ${table} WHERE timestamp >= ? ORDER BY timestamp ASC`).all(since);
      odb.close();
      res.json({ readings: rows });
    } catch (err) { res.status(500).json({ error: err.message, readings: [] }); }
  };
}
app.get('/api/sp500-history', apiLimiter, requireAuth, overlayHandler('spx'));
app.get('/api/gold-history',  apiLimiter, requireAuth, overlayHandler('gold'));
app.get('/api/btc-history',   apiLimiter, requireAuth, overlayHandler('btc'));

// Auto Market Data (monthly, 3 lines: Overall / EV / Non-EV, base 1000 at Jan 2020)
// Returns { overall: [...], ev: [...], nonev: [...] } — each an array of
// { timestamp, value } points. Frontend toggles which lines are visible.
app.get('/api/auto-market-data', apiLimiter, requireAuth, (req, res) => {
  try {
    const since = getSince(req.query.range || 'MAX');
    const series = amd.getSeries(db, since);
    // Back-compat: legacy clients expect `readings` (overall line only).
    res.json({
      overall: series.overall,
      ev:      series.ev,
      nonev:   series.nonev,
      readings: series.overall,
      base: { value: 1000, date: '2020-01-01' },
    });
  } catch (err) { res.status(500).json({ error: err.message, overall: [], ev: [], nonev: [], readings: [] }); }
});

// Latest point per line for hero display
app.get('/api/auto-market-data/latest', apiLimiter, requireAuth, (req, res) => {
  try { res.json(amd.getLatest(db) || {}); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// ---------------------------------------------------------------------------
// Monthly rebalance with splice continuity
// ---------------------------------------------------------------------------
db.exec(`
  CREATE TABLE IF NOT EXISTS rebalance_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    added_json TEXT,
    dropped_json TEXT,
    pre_index REAL,
    post_index REAL,
    pre_scale REAL,
    post_scale REAL
  );
  CREATE TABLE IF NOT EXISTS model_status (
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    segment TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    is_bench INTEGER NOT NULL DEFAULT 0,
    dropped_at TEXT,
    promoted_at TEXT,
    PRIMARY KEY (make, model)
  );
  CREATE TABLE IF NOT EXISTS index_scale (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    scale REAL NOT NULL DEFAULT 1.0
  );
  -- Per-segment history. Stores live monthly polls + Manheim-derived
  -- backfill (segment source = 'manheim' vs 'live'). Also stores pseudo-
  -- segments 'suv_total', 'car_total', 'ami_composite' for unified querying.
  CREATE TABLE IF NOT EXISTS auto_segment_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    segment TEXT NOT NULL,
    median_price REAL,
    model_count INTEGER,
    source TEXT NOT NULL DEFAULT 'live'
  );
  CREATE INDEX IF NOT EXISTS idx_auto_seg_hist_ts ON auto_segment_history(timestamp);
  CREATE INDEX IF NOT EXISTS idx_auto_seg_hist_seg ON auto_segment_history(segment);
  -- Per-segment splice factor (methodology v2.1 §9.2). Multiplies backfill
  -- series to align continuity with the first live poll.
  CREATE TABLE IF NOT EXISTS segment_splice_factors (
    segment TEXT PRIMARY KEY,
    splice_factor REAL NOT NULL,
    backfill_last_value REAL,
    live_first_value REAL,
    computed_at TEXT NOT NULL
  );
  -- Change log for basket edits (add/drop/reclassify models)
  CREATE TABLE IF NOT EXISTS auto_basket_changelog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    changed_at TEXT NOT NULL,
    action TEXT NOT NULL,   -- 'add' | 'drop' | 'reclassify' | 'bench' | 'unbench'
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    old_segment TEXT,
    new_segment TEXT,
    reason TEXT
  );
`);

// Migration: add 'segment' column to model_status if missing (idempotent)
(function migrateModelStatusSegment() {
  const cols = db.prepare("PRAGMA table_info(model_status)").all();
  const hasSegment = cols.some(c => c.name === 'segment');
  if (!hasSegment) {
    db.exec('ALTER TABLE model_status ADD COLUMN segment TEXT');
    console.log('[auto] migration: added model_status.segment column');
  }
})();

// Seed / refresh model_status from MODELS list.
// On first run: insert all 185 rows as active, non-benched.
// On subsequent runs: insert any MODELS missing from the table; refresh
// segment for existing rows (keeps segment column in sync with basket doc).
(function seedModelStatus() {
  const ins = db.prepare(`INSERT OR IGNORE INTO model_status
    (make, model, segment, is_active, is_bench) VALUES (?, ?, ?, 1, 0)`);
  const updSeg = db.prepare('UPDATE model_status SET segment = ? WHERE make = ? AND model = ?');
  let added = 0, updated = 0;
  const tx = db.transaction(() => {
    for (const m of MODELS) {
      const existed = db.prepare('SELECT segment FROM model_status WHERE make = ? AND model = ?').get(m.make, m.model);
      if (!existed) {
        ins.run(m.make, m.model, m.segment);
        added++;
      } else if (existed.segment !== m.segment) {
        updSeg.run(m.segment, m.make, m.model);
        updated++;
      }
    }
  });
  tx();
  db.prepare('INSERT OR IGNORE INTO index_scale (id, scale) VALUES (1, 1.0)').run();
  const total = db.prepare('SELECT COUNT(*) AS c FROM model_status').get().c;
  console.log(`[auto] model_status: ${total} rows total (added=${added}, segment-updated=${updated}, basket size=${MODELS.length})`);
})();

app.get('/api/rebalance-history', apiLimiter, requireAuth, (req, res) => {
  try {
    const events = db.prepare('SELECT * FROM rebalance_events ORDER BY run_at DESC LIMIT 50').all();
    res.json({ events });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

async function monthlyRebalance() {
  console.log('[auto] === monthly rebalance start ===');
  const runAt = new Date().toISOString();

  // Identify zero-listing models over last 30 days
  const thirtyAgo = new Date(Date.now() - 30*86400000).toISOString();
  const zeroVol = db.prepare(`
    SELECT ms.make, ms.model
    FROM model_status ms
    LEFT JOIN (
      SELECT make, model, SUM(COALESCE(listing_count,0)) AS v
      FROM auto_prices WHERE timestamp >= ? GROUP BY make, model
    ) ap ON ap.make = ms.make AND ap.model = ms.model
    WHERE ms.is_active = 1 AND COALESCE(ap.v, 0) = 0
  `).all(thirtyAgo);

  const pre = db.prepare('SELECT index_value FROM auto_index ORDER BY timestamp DESC LIMIT 1').get();
  const preIndex = pre ? pre.index_value : null;
  const preScaleRow = db.prepare('SELECT scale FROM index_scale WHERE id = 1').get();
  const preScale = preScaleRow ? preScaleRow.scale : 1.0;

  // The 25-model basket is a fixed list today. Rebalance = drop zero-volume and
  // log the event. (Adding new models requires a code change to MODELS[].)
  const dropStmt = db.prepare('UPDATE model_status SET is_active = 0, dropped_at = ? WHERE make = ? AND model = ?');
  const tx = db.transaction(() => { for (const d of zeroVol) dropStmt.run(runAt, d.make, d.model); });
  tx();

  await pollAllModels();

  const post = db.prepare('SELECT index_value FROM auto_index ORDER BY timestamp DESC LIMIT 1').get();
  const rawPostIndex = post ? post.index_value / preScale : null;
  let postScale = preScale;
  if (preIndex && rawPostIndex && rawPostIndex > 0) postScale = preIndex / rawPostIndex;

  if (post) {
    const spliced = Math.round(rawPostIndex * postScale * 100) / 100;
    db.prepare('UPDATE auto_index SET index_value = ? WHERE timestamp = (SELECT MAX(timestamp) FROM auto_index)').run(spliced);
    db.prepare('UPDATE index_scale SET scale = ? WHERE id = 1').run(postScale);
  }

  db.prepare(`INSERT INTO rebalance_events
    (run_at, added_json, dropped_json, pre_index, post_index, pre_scale, post_scale)
    VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run(runAt, '[]', JSON.stringify(zeroVol), preIndex, preIndex, preScale, postScale);

  console.log(`[auto] rebalance done. dropped=${zeroVol.length} preIndex=${preIndex} scale=${preScale}→${postScale}`);
}

let lastAutoRebalanceKey = null;
setInterval(async () => {
  const now = new Date();
  if (now.getUTCDate() === 1 && now.getUTCHours() === 3) {
    const key = `${now.getUTCFullYear()}-${now.getUTCMonth()}`;
    if (lastAutoRebalanceKey !== key) {
      lastAutoRebalanceKey = key;
      try { await monthlyRebalance(); } catch (e) { console.error('[auto] rebalance err:', e); }
    }
  }
}, 60 * 60 * 1000);

// ---------------------------------------------------------------------------
// Scheduling: MONTHLY poll (first UTC day of each month at 03:00 UTC).
// Also triggers on startup if auto_prices is empty.
// Note: monthlyRebalance() upstream handles drop-zero-volume + splice on the
// same day-1 hour-3 cadence; scheduling here intentionally aligns with that
// so both run in the same window.
// ---------------------------------------------------------------------------
const HOUR_MS = 60 * 60 * 1000;

// Startup poll if DB is empty (bootstrap only).
setTimeout(async () => {
  const count = db.prepare('SELECT COUNT(*) as cnt FROM auto_prices').get().cnt;
  if (count === 0) {
    console.log('[startup] auto_prices table empty — running initial poll...');
    await pollAllModels();
  } else {
    console.log(`[startup] auto_prices has ${count} rows — skipping initial poll`);
  }
}, 5000);

// Monthly recurring poll: check hourly; fire once when UTC-day=1 and hour=4
// (one hour after monthlyRebalance so we don't race). Keyed by year-month to
// guarantee single execution.
let lastMonthlyPollKey = null;
setInterval(async () => {
  const now = new Date();
  if (now.getUTCDate() === 1 && now.getUTCHours() === 4) {
    const key = `${now.getUTCFullYear()}-${now.getUTCMonth()}`;
    if (lastMonthlyPollKey !== key) {
      lastMonthlyPollKey = key;
      try { await pollAllModels(); } catch (e) { console.error('[auto] monthly poll err:', e); }
    }
  }
}, HOUR_MS);

// Auto Market Data: monthly refresh — try once at startup (delayed) and then daily thereafter.
// Publisher releases new months on ~5th business day of the following month.
async function parseXlsxViaPython(db, xlsxPath) {
  return new Promise((resolve, reject) => {
    const py = spawn('python3', [path.join(__dirname, 'scripts', 'parse_xlsx_to_stdin.py'), xlsxPath]);
    let out = '', err = '';
    py.stdout.on('data', d => out += d);
    py.stderr.on('data', d => err += d);
    py.on('close', code => {
      if (code !== 0) return reject(new Error(`python exit ${code}: ${err}`));
      try {
        const rows = JSON.parse(out);
        const now = new Date().toISOString();
        const ins = db.prepare(`INSERT OR REPLACE INTO auto_market_data_monthly
          (date, index_value, index_usd_sa, seasonal_factor, index_usd_nsa, mom_pct, yoy_pct, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
        let added = 0;
        const tx = db.transaction(() => {
          for (const r of rows) {
            const date = String(r.date).slice(0, 10);
            const idx = Number(r['Index (1/97 = 100)']);
            if (!Number.isFinite(idx)) continue;
            const before = db.prepare('SELECT 1 FROM auto_market_data_monthly WHERE date = ?').get(date);
            ins.run(date, idx,
              num(r['Manheim Index $ amount SA']),
              num(r['Seasonal adjustment factor']),
              num(r['Manheim Index $ amount NSA']),
              num(r['SA Price % MoM'] ?? r['Index % MoM']),
              num(r['Index % YoY'] ?? r['NSA Price % YoY']),
              now);
            if (!before) added++;
          }
        });
        tx();
        resolve(added);
      } catch (e) { reject(e); }
    });
  });
}
function num(v) { if (v == null) return null; const n = Number(v); return Number.isFinite(n) ? n : null; }

setTimeout(() => {
  amd.refreshLatest(db, parseXlsxViaPython)
    .then(msg => console.log(`[auto-market-data] startup refresh: ${msg}`))
    .catch(e => console.warn(`[auto-market-data] startup refresh error: ${e.message}`));
}, 15000);
const DAY_MS = 24 * 3600 * 1000;
setInterval(() => {
  amd.refreshLatest(db, parseXlsxViaPython)
    .then(msg => console.log(`[auto-market-data] daily refresh: ${msg}`))
    .catch(e => console.warn(`[auto-market-data] daily refresh error: ${e.message}`));
}, DAY_MS);

// ---------------------------------------------------------------------------
// Start server
// ---------------------------------------------------------------------------
app.listen(PORT, '127.0.0.1', () => {
  console.log(`Auto Market Index Dashboard running on http://localhost:${PORT}`);
  console.log(`Tracking ${MODELS.length} models | Weekly polling | Port ${PORT}`);
});
