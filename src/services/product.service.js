const fs = require('fs');
const path = require('path');
const globetopperProvider = require('../providers/globetopper.provider');
const dtoneProvider = require('../providers/dtone.provider');
const ppnProvider = require('../providers/ppn.provider');
const billersProvider = require('../providers/billers.provider');
const logger = require('../utils/logger');

// ─── Disk-Persisted Cache ────────────────────────────────────────────────────
const CACHE_DIR = path.join(__dirname, '../../cache');
const CACHE_FILE = path.join(CACHE_DIR, 'products_cache.json');

/**
 * Load cache from disk on startup — provides instant availability of all products
 */
function loadCacheFromDisk() {
  try {
    if (!fs.existsSync(CACHE_FILE)) {
      logger.info('No disk cache file found, will rely on background warm-up');
      return false;
    }
    const raw = fs.readFileSync(CACHE_FILE, 'utf-8');
    const saved = JSON.parse(raw);

    let loaded = 0;
    for (const provider of ['globetopper', 'dtone', 'ppn', 'billers']) {
      if (saved[provider] && Array.isArray(saved[provider].products) && saved[provider].products.length > 0) {
        productCache[provider] = {
          products: saved[provider].products,
          timestamp: Date.now()  // Mark as fresh so isCacheReady() returns true
        };
        loaded += saved[provider].products.length;
      }
    }

    if (loaded > 0) {
      logger.info(`Disk cache loaded: ${loaded} products available instantly`);
      return true;
    }
    logger.info('Disk cache was empty, will rely on background warm-up');
    return false;
  } catch (err) {
    logger.warn('Failed to load disk cache', { error: err.message });
    return false;
  }
}

/**
 * Save current in-memory cache to disk for instant loading on next startup
 */
function saveCacheToDisk() {
  try {
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }
    const dataToSave = {};
    for (const provider of ['globetopper', 'dtone', 'ppn', 'billers']) {
      dataToSave[provider] = {
        products: productCache[provider].products,
        savedAt: new Date().toISOString()
      };
    }
    fs.writeFileSync(CACHE_FILE, JSON.stringify(dataToSave));
    const total = Object.values(dataToSave).reduce((sum, c) => sum + c.products.length, 0);
    logger.info(`Disk cache saved: ${total} products written to ${CACHE_FILE}`);
  } catch (err) {
    logger.warn('Failed to save disk cache', { error: err.message });
  }
}

/**
 * Normalize GlobeTopper product to orchestrator format
 */
function normalizeGlobeTopper(catalogueItems, productsData) {
  // Merge catalogue items with matching products
  const usedProductIds = new Set();
  const merged = catalogueItems.map(catItem => {
    const productData = productsData.find(p => p.operator?.id === catItem.topup_product_id);
    if (productData?.operator?.id) usedProductIds.add(productData.operator.id);
    return { ...catItem, ...productData };
  });

  // Include products that don't have matching catalogue entries
  const unmatchedProducts = productsData.filter(p => !usedProductIds.has(p.operator?.id));
  const allProducts = [...merged, ...unmatchedProducts];

  return allProducts.map(p => ({
    ...p,
    provider: 'globetopper',
    providerLabel: 'GlobeTopper',
    productId: String(p.sku || p.id || p.BillerID || ''),
    productName: p.name || '',
    topup_product_id: p.topup_product_id || p.operator?.id,
  }));
}

/**
 * Normalize DT-One product to orchestrator format
 */
function normalizeDtOne(products) {
  if (!Array.isArray(products)) return [];

  return products.map(p => {
    // Country comes from operator.country, NOT destination.country
    const countryIso = p.operator?.country?.iso_code || '';
    const countryName = p.operator?.country?.name || '';
    const opName = p.operator?.name || p.name || '';
    const serviceName = p.service?.name || p.type || 'Topup';
    const prefixedId = 'dtone_' + (p.id || '');

    // DT-One: destination.amount is a NUMBER, destination.unit is the currency
    const destAmount = p.destination?.amount || 0;
    const destCurr = p.destination?.unit || '';
    const sourceAmount = p.source?.amount || 0;
    const sourceCurr = p.source?.unit || '';
    const retailPrice = p.prices?.retail?.amount || sourceAmount;
    const retailCurr = p.prices?.retail?.unit || sourceCurr;

    // Build denomination string
    let denomination = '';
    if (destAmount) {
      denomination = `${destAmount} ${destCurr}`;
    } else if (retailPrice) {
      denomination = `${retailPrice} ${retailCurr}`;
    }

    return {
      ...p,
      provider: 'dtone',
      providerLabel: 'DT-One',
      productId: String(p.id || ''),
      productName: p.name || opName,
      brand: opName,
      name: p.name || opName,
      BillerID: prefixedId,
      iso2: countryIso,
      country: countryName,
      countryCode: countryIso,
      operator: {
        id: prefixedId,
        name: opName,
        logo_url: p.operator?.logo_url || null,
        country: { iso2: countryIso, name: countryName, currency: { code: destCurr || retailCurr, name: destCurr || retailCurr } }
      },
      category: { id: 1001, name: serviceName, description: serviceName },
      type: { id: 1001, name: p.type || serviceName },
      currency: { code: destCurr || retailCurr, name: destCurr || retailCurr },
      min: String(retailPrice || destAmount),
      max: String(retailPrice || destAmount),
      denominations: destAmount ? [destAmount] : [],
      denomination,
      user_display: denomination,
      card_image: p.operator?.logo_url || null,
      imageUrl: p.operator?.logo_url || null,
    };
  });
}

/**
 * Normalize PPN product to orchestrator format
 */
function normalizePpn(data) {
  const products = Array.isArray(data) ? data : (data?.payLoad || data?.skus || data?.data || []);

  return products.map(p => {
    const skuCode = p.SkuCode || p.skuId || p.sku_code || p.id || '';
    const prefixedId = 'ppn_' + skuCode;
    const productName = p.ProductName || p.productName || p.product_name || p.name || '';
    const brandName = p.Brand || p.brand || p.OperatorName || p.operatorName || '';
    const countryCode = p.CountryCode || p.countryCode || p.country_code || '';
    const countryName = p.Country || p.country || '';
    const currencyCode = p.Currency || p.currency || '';
    const categoryName = p.Category || p.category || 'Topup';
    const typeName = p.Type || p.type || categoryName;
    const logoUrl = p.LogoUrl || p.logoUrl || p.logo_url || p.ImageUrl || null;
    const minAmt = p.MinAmount || p.minAmount || p.min || 0;
    const maxAmt = p.MaxAmount || p.maxAmount || p.max || 0;
    const fixedAmounts = p.FixedAmounts || p.fixedAmounts || p.denominations || [];

    let denomination = '';
    if (fixedAmounts.length > 0) {
      denomination = fixedAmounts.map(a => `${a} ${currencyCode}`).join(', ');
    } else if (minAmt && maxAmt) {
      denomination = `${minAmt} - ${maxAmt} ${currencyCode}`;
    }

    return {
      ...p,
      provider: 'ppn',
      providerLabel: 'PPN',
      productId: String(skuCode),
      productName,
      brand: brandName || productName,
      name: productName,
      BillerID: prefixedId,
      iso2: countryCode,
      country: countryName,
      countryCode,
      operator: {
        id: prefixedId,
        name: brandName || productName,
        logo_url: logoUrl,
        country: { iso2: countryCode, name: countryName, currency: { code: currencyCode, name: currencyCode } }
      },
      category: { id: 2001, name: categoryName, description: categoryName },
      type: { id: 2001, name: typeName },
      currency: { code: currencyCode, name: currencyCode },
      min: String(minAmt),
      max: String(maxAmt),
      denominations: fixedAmounts,
      denomination,
      user_display: denomination,
      card_image: logoUrl,
      imageUrl: logoUrl,
    };
  });
}

/**
 * Normalize Billers product to orchestrator format
 */
// Map country codes (ISO2 + ISO3) to common currencies
const COUNTRY_CURRENCY_MAP = {
  // ISO2
  US: 'USD', GB: 'GBP', CA: 'CAD', AU: 'AUD', IN: 'INR', AE: 'AED', SA: 'SAR',
  MX: 'MXN', BR: 'BRL', NG: 'NGN', KE: 'KES', GH: 'GHS', ZA: 'ZAR', EG: 'EGP',
  PH: 'PHP', PK: 'PKR', BD: 'BDT', ID: 'IDR', MY: 'MYR', TH: 'THB', VN: 'VND',
  CO: 'COP', AR: 'ARS', CL: 'CLP', PE: 'PEN', TR: 'TRY', JP: 'JPY', KR: 'KRW',
  CN: 'CNY', SG: 'SGD', HK: 'HKD', TW: 'TWD', NZ: 'NZD', SE: 'SEK', NO: 'NOK',
  DK: 'DKK', CH: 'CHF', EU: 'EUR', DE: 'EUR', FR: 'EUR', IT: 'EUR', ES: 'EUR',
  NL: 'EUR', PT: 'EUR', BE: 'EUR', AT: 'EUR', IE: 'EUR', FI: 'EUR', GR: 'EUR',
  JO: 'JOD', KW: 'KWD', BH: 'BHD', QA: 'QAR', OM: 'OMR', LB: 'LBP', IQ: 'IQD',
  TZ: 'TZS', UG: 'UGX', RW: 'RWF', ET: 'ETB', CM: 'XAF', SN: 'XOF', MM: 'MMK',
  CI: 'XOF', ML: 'XOF', BF: 'XOF', NE: 'XOF', TG: 'XOF', BJ: 'XOF', LK: 'LKR',
  NP: 'NPR', KH: 'KHR', LA: 'LAK', GT: 'GTQ', HN: 'HNL', SV: 'USD', NI: 'NIO',
  CR: 'CRC', PA: 'PAB', DO: 'DOP', JM: 'JMD', TT: 'TTD', HT: 'HTG',
  // ISO3 (Billers API uses these)
  USA: 'USD', GBR: 'GBP', CAN: 'CAD', AUS: 'AUD', IND: 'INR', ARE: 'AED', SAU: 'SAR',
  MEX: 'MXN', BRA: 'BRL', NGA: 'NGN', KEN: 'KES', GHA: 'GHS', ZAF: 'ZAR', EGY: 'EGP',
  PHL: 'PHP', PAK: 'PKR', BGD: 'BDT', IDN: 'IDR', MYS: 'MYR', THA: 'THB', VNM: 'VND',
  COL: 'COP', ARG: 'ARS', CHL: 'CLP', PER: 'PEN', TUR: 'TRY', JPN: 'JPY', KOR: 'KRW',
  CHN: 'CNY', SGP: 'SGD', HKG: 'HKD', TWN: 'TWD', NZL: 'NZD', SWE: 'SEK', NOR: 'NOK',
  DNK: 'DKK', CHE: 'CHF', DEU: 'EUR', FRA: 'EUR', ITA: 'EUR', ESP: 'EUR', NLD: 'EUR',
  PRT: 'EUR', BEL: 'EUR', AUT: 'EUR', IRL: 'EUR', FIN: 'EUR', GRC: 'EUR',
  JOR: 'JOD', KWT: 'KWD', BHR: 'BHD', QAT: 'QAR', OMN: 'OMR', LBN: 'LBP', IRQ: 'IQD',
  TZA: 'TZS', UGA: 'UGX', RWA: 'RWF', ETH: 'ETB', CMR: 'XAF', SEN: 'XOF', MMR: 'MMK',
  CIV: 'XOF', MLI: 'XOF', BFA: 'XOF', NER: 'XOF', TGO: 'XOF', BEN: 'XOF', LKA: 'LKR',
  NPL: 'NPR', KHM: 'KHR', LAO: 'LAK', GTM: 'GTQ', HND: 'HNL', SLV: 'USD', NIC: 'NIO',
  CRI: 'CRC', PAN: 'PAB', DOM: 'DOP', JAM: 'JMD', TTO: 'TTD', HTI: 'HTG',
  // Worldwide / Global
  WW: 'USD', WWX: 'USD', XX: 'USD',
};

// Map raw country codes to readable names (for codes the API returns without names)
const COUNTRY_NAME_MAP = {
  WW: 'Worldwide', WWX: 'Worldwide', XX: 'Global',
};

function normalizeBillers(data) {
  const billers = Array.isArray(data) ? data : (data?.Data || data?.Billers || data?.data || []);

  return billers.map(p => {
    const billerId = p.BillerID || p.billerId || p.id || '';
    const prefixedId = 'billers_' + billerId;
    const billerName = p.BillerName || p.billerName || p.name || '';
    const rawCountryCode = p.CountryCode || p.countryCode || '';
    const countryCode = rawCountryCode;
    const countryName = COUNTRY_NAME_MAP[rawCountryCode] || p.CountryName || p.countryName || rawCountryCode;
    const currencyCode = p.Currency || p.currency || COUNTRY_CURRENCY_MAP[rawCountryCode] || '';
    const categoryName = p.BillerType || p.CategoryCode || p.billerType || 'Bill Payment';
    const typeName = p.BillerSubType || p.billerSubType || categoryName;
    const logoUrl = p.Logo || p.LogoUrl || p.logoUrl || null;
    const minAmt = p.MinAmount || p.minAmount || p.min || 0;
    const maxAmt = p.MaxAmount || p.maxAmount || p.max || 0;
    const backendFee = p.BackendFee || 0;

    let denomination = '';
    if (minAmt && maxAmt) {
      denomination = `${minAmt} - ${maxAmt} ${currencyCode}`;
    } else if (currencyCode) {
      denomination = `Variable (${currencyCode})`;
    }

    return {
      ...p,
      provider: 'billers',
      providerLabel: 'Billers',
      productId: String(billerId),
      productName: billerName,
      brand: billerName,
      name: billerName,
      BillerID: prefixedId,
      iso2: countryCode,
      country: countryName,
      countryCode,
      operator: {
        id: prefixedId,
        name: billerName,
        logo_url: logoUrl,
        country: { iso2: countryCode, name: countryName, currency: { code: currencyCode, name: currencyCode } }
      },
      category: { id: 3001, name: categoryName, description: categoryName },
      type: { id: 3001, name: typeName },
      currency: { code: currencyCode, name: currencyCode },
      min: String(minAmt),
      max: String(maxAmt),
      denominations: [],
      denomination,
      user_display: denomination,
      card_image: logoUrl,
      imageUrl: logoUrl,
    };
  });
}

// ─── In-Memory Product Cache ─────────────────────────────────────────────────
const productCache = {
  globetopper: { products: [], timestamp: 0 },
  dtone: { products: [], timestamp: 0 },
  ppn: { products: [], timestamp: 0 },
  billers: { products: [], timestamp: 0 },
};
const CACHE_TTL = 30 * 60 * 1000; // 30 minutes
let cacheWarming = false;

function isCacheReady(provider) {
  return productCache[provider].products.length > 0 && (Date.now() - productCache[provider].timestamp < CACHE_TTL);
}

/**
 * Check if provider has ANY cached data (stale or fresh)
 * Used to serve stale data immediately while refreshing in background
 */
function hasCacheData(provider) {
  return productCache[provider].products.length > 0;
}

/**
 * Trigger background warm-up if cache is stale (non-blocking)
 */
function triggerBackgroundRefreshIfStale() {
  const anyStale = ['globetopper', 'dtone', 'ppn', 'billers'].some(p => !isCacheReady(p) && hasCacheData(p));
  if (anyStale && !cacheWarming) {
    logger.info('Stale cache detected, triggering background refresh');
    warmUpCache().catch(err => logger.warn('Background refresh failed', { error: err.message }));
  }
}

/**
 * Background cache warm-up - fetches ALL pages from all providers
 * Runs on startup and every 30 minutes
 */
async function warmUpCache() {
  if (cacheWarming) return;
  cacheWarming = true;
  logger.info('Cache warm-up started...');

  // GlobeTopper (no pagination)
  try {
    const [catalogue, products] = await Promise.all([
      globetopperProvider.getCatalogue({}),
      globetopperProvider.getProducts({})
    ]);
    productCache.globetopper = { products: normalizeGlobeTopper(catalogue, products), timestamp: Date.now() };
    logger.info(`Cache warmed: GlobeTopper = ${productCache.globetopper.products.length} products`);
  } catch (err) {
    logger.warn('Cache warm-up failed: GlobeTopper', { error: err.message });
  }

  // DT-One (all pages - ~7000 products)
  try {
    const allDtProducts = await dtoneProvider.getAllProducts({});
    const existingCount = productCache.dtone.products.length;
    // Only update cache if we got more products than before (avoid partial overwrites)
    if (allDtProducts.length >= existingCount || existingCount === 0) {
      productCache.dtone = { products: normalizeDtOne(allDtProducts), timestamp: Date.now() };
      logger.info(`Cache warmed: DT-One = ${productCache.dtone.products.length} products`);
    } else {
      logger.warn(`DT-One warm-up returned fewer products (${allDtProducts.length}) than cached (${existingCount}), keeping existing cache`);
    }
  } catch (err) {
    logger.warn('Cache warm-up failed: DT-One', { error: err.message });
  }

  // Billers (all pages - ~2400 products)
  try {
    const allBillers = await billersProvider.getAllBillers({});
    const existingBillersCount = productCache.billers.products.length;
    if (allBillers.length >= existingBillersCount || existingBillersCount === 0) {
      productCache.billers = { products: normalizeBillers(allBillers), timestamp: Date.now() };
      logger.info(`Cache warmed: Billers = ${productCache.billers.products.length} products`);
    } else {
      logger.warn(`Billers warm-up returned fewer products (${allBillers.length}) than cached (${existingBillersCount}), keeping existing cache`);
    }
  } catch (err) {
    logger.warn('Cache warm-up failed: Billers', { error: err.message });
  }

  // PPN
  if (process.env.PPN_ENABLED !== 'false') {
    try {
      const ppnData = await ppnProvider.getProducts({});
      productCache.ppn = { products: normalizePpn(ppnData), timestamp: Date.now() };
      logger.info(`Cache warmed: PPN = ${productCache.ppn.products.length} products`);
    } catch (err) {
      logger.warn('Cache warm-up failed: PPN', { error: err.message });
    }
  }

  cacheWarming = false;
  const total = Object.values(productCache).reduce((sum, c) => sum + c.products.length, 0);
  logger.info(`Cache warm-up complete: ${total} total products cached`);

  // Save to disk so next restart loads instantly
  saveCacheToDisk();
}

// Load from disk first (instant availability), then warm-up in background to refresh
loadCacheFromDisk();

// Start background warm-up 5 seconds after startup
setTimeout(warmUpCache, 5000);
// Refresh every 30 minutes
setInterval(warmUpCache, CACHE_TTL);

// ─── Category Normalization ──────────────────────────────────────────────────
// Maps raw category names from all providers to a single canonical name.
// Key = lowercased/trimmed variant, Value = canonical display name.
const CATEGORY_ALIAS_MAP = {
  // eSIM variants
  'esim': 'eSIM',
  // Gift Card variants
  'gift cards': 'Gift Card',
  'giftcard': 'Gift Card',
  'giftcards': 'Gift Card',
  'gift card': 'Gift Card',
  // Top-Up variants
  'top-up': 'Top-Up',
  'top up': 'Top-Up',
  'topup': 'Top-Up',
  // Charity / Donation variants
  'charity & donations': 'Charity',
  'charity and donations': 'Charity',
  'donation': 'Charity',
  'donations': 'Charity',
  // Telecom variants
  'telecommunications': 'Telecom',
  'telecoms & media': 'Telecom',
  'telecoms and media': 'Telecom',
  // Transport variants
  'transportation': 'Transport',
  // Utility variants
  'utility': 'Utilities',
  'electric utility': 'Utilities',
  'water utility': 'Utilities',
  'tv,utility': 'Utilities',
  'tv, utility': 'Utilities',
  // Internet variants
  'cable and internet': 'Internet',
  'cable & internet': 'Internet',
  // Mobile variants
  'mobile postpaid': 'Mobile',
};

/**
 * Normalize a category name to its canonical form.
 * Handles case, spacing, plural, and semantic duplicates.
 */
function normalizeCategory(rawName) {
  if (!rawName || typeof rawName !== 'string') return 'Other';
  const trimmed = rawName.trim();
  if (!trimmed) return 'Other';

  const key = trimmed.toLowerCase().replace(/\s+/g, ' ');
  if (CATEGORY_ALIAS_MAP[key]) return CATEGORY_ALIAS_MAP[key];

  // No alias found — return original with consistent Title Case first letter
  return trimmed;
}

/**
 * Ensure product has all required orchestrator fields
 */
function ensureOrchestratorFields(p) {
  if (!p.operator || typeof p.operator !== 'object') {
    p.operator = {
      id: p.BillerID || p.productId || p.id || '',
      name: p.productName || p.name || p.brand || '',
      logo_url: p.imageUrl || p.card_image || null,
      country: { iso2: p.iso2 || p.countryCode || '', name: p.country || '', currency: { code: p.currency?.code || '', name: p.currency?.name || '' } }
    };
  }
  if (!p.category || typeof p.category !== 'object') {
    p.category = { id: 9999, name: p.category || 'Other', description: p.category || 'Other' };
  }
  // Normalize category name across all providers
  const canonical = normalizeCategory(p.category.name);
  p.category.name = canonical;
  p.category.description = canonical;

  if (!p.providerLabel) {
    p.providerLabel = p.provider || 'Unknown';
  }
  return p;
}

/**
 * Fetch products from ALL 4 providers, normalize, filter, paginate
 * Uses cache for full dataset, falls back to first-page fetch if cache not ready
 */
const getAllProducts = async (filters = {}) => {
  const { page, limit, country, category, search, provider: providerFilter } = filters;

  logger.info('Fetching unified products from all providers', { filters, cacheStatus: {
    globetopper: isCacheReady('globetopper') ? productCache.globetopper.products.length : 'not ready',
    dtone: isCacheReady('dtone') ? productCache.dtone.products.length : 'not ready',
    billers: isCacheReady('billers') ? productCache.billers.products.length : 'not ready',
    ppn: isCacheReady('ppn') ? productCache.ppn.products.length : 'not ready',
  }});

  let allProducts = [];
  const providerCounts = {};

  // ── GlobeTopper ──
  if (!providerFilter || providerFilter === 'globetopper') {
    if (isCacheReady('globetopper') || hasCacheData('globetopper')) {
      let cached = productCache.globetopper.products;
      if (country) cached = cached.filter(p => p.iso2 === country || p.countryCode === country || p.operator?.country?.iso2 === country);
      providerCounts.globetopper = cached.length;
      allProducts = allProducts.concat(cached);
    } else {
      try {
        const [catalogue, products] = await Promise.all([
          globetopperProvider.getCatalogue({ countryCode: country }),
          globetopperProvider.getProducts({ countryCode: country })
        ]);
        const normalized = normalizeGlobeTopper(catalogue, products);
        logger.info('GlobeTopper fetched (live)', { count: normalized.length });
        providerCounts.globetopper = normalized.length;
        allProducts = allProducts.concat(normalized);
      } catch (err) {
        logger.warn('GlobeTopper fetch failed', { error: err.message });
        providerCounts.globetopper = 0;
      }
    }
  }

  // ── DT-One ──
  if (!providerFilter || providerFilter === 'dtone') {
    if (isCacheReady('dtone') || hasCacheData('dtone')) {
      let cached = productCache.dtone.products;
      if (country) cached = cached.filter(p => p.iso2 === country || p.countryCode === country);
      providerCounts.dtone = cached.length;
      allProducts = allProducts.concat(cached);
    } else {
      try {
        const allDtProducts = await dtoneProvider.getAllProducts({ country });
        const normalized = normalizeDtOne(allDtProducts);
        logger.info('DT-One fetched (live, all pages)', { count: normalized.length });
        providerCounts.dtone = normalized.length;
        allProducts = allProducts.concat(normalized);
      } catch (err) {
        logger.warn('DT-One fetch failed', { error: err.message });
        providerCounts.dtone = 0;
      }
    }
  }

  // ── PPN ──
  if (!providerFilter || providerFilter === 'ppn') {
    if (process.env.PPN_ENABLED === 'false') {
      logger.info('PPN provider disabled (PPN_ENABLED=false)');
      providerCounts.ppn = 0;
    } else if (isCacheReady('ppn') || hasCacheData('ppn')) {
      let cached = productCache.ppn.products;
      if (country) cached = cached.filter(p => p.iso2 === country || p.countryCode === country);
      providerCounts.ppn = cached.length;
      allProducts = allProducts.concat(cached);
    } else {
      try {
        const data = await ppnProvider.getProducts({ country });
        const normalized = normalizePpn(data);
        providerCounts.ppn = normalized.length;
        allProducts = allProducts.concat(normalized);
      } catch (err) {
        logger.warn('PPN fetch failed', { error: err.message });
        providerCounts.ppn = 0;
      }
    }
  }

  // ── Billers ──
  if (!providerFilter || providerFilter === 'billers') {
    if (isCacheReady('billers') || hasCacheData('billers')) {
      let cached = productCache.billers.products;
      if (country) cached = cached.filter(p => p.iso2 === country || p.countryCode === country);
      providerCounts.billers = cached.length;
      allProducts = allProducts.concat(cached);
    } else {
      try {
        const allBillersData = await billersProvider.getAllBillers(country ? { CountryCode: country } : {});
        const normalized = normalizeBillers(allBillersData);
        logger.info('Billers fetched (live, all pages)', { count: normalized.length });
        providerCounts.billers = normalized.length;
        allProducts = allProducts.concat(normalized);
      } catch (err) {
        logger.warn('Billers fetch failed', { error: err.message });
        providerCounts.billers = 0;
      }
    }
  }

  // Trigger background refresh if any provider cache is stale
  triggerBackgroundRefreshIfStale();

  // Ensure all products have orchestrator-required fields
  allProducts = allProducts.map(ensureOrchestratorFields);

  // Apply text filters
  if (category) {
    const cat = category.toLowerCase();
    allProducts = allProducts.filter(p => {
      const catName = typeof p.category === 'object' ? p.category?.name : p.category;
      return catName?.toLowerCase().includes(cat);
    });
  }

  if (search) {
    const s = search.toLowerCase();
    allProducts = allProducts.filter(p =>
      p.productName?.toLowerCase().includes(s) ||
      p.brand?.toLowerCase().includes(s) ||
      p.description?.toLowerCase().includes(s)
    );
  }

  // Paginate only if limit is provided, otherwise return ALL products
  const total = allProducts.length;

  if (limit) {
    const pageNum = parseInt(page) || 1;
    const limitNum = parseInt(limit);
    const startIndex = (pageNum - 1) * limitNum;
    const paginatedProducts = allProducts.slice(startIndex, startIndex + limitNum);

    return {
      products: paginatedProducts,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages: Math.ceil(total / limitNum)
      },
      providers: providerCounts
    };
  }

  // No limit specified — return ALL products (orchestrator handles its own pagination)
  return {
    products: allProducts,
    pagination: {
      page: 1,
      limit: total,
      total,
      totalPages: 1
    },
    providers: providerCounts
  };
};

/**
 * Get a single product by ID - routes to correct provider based on prefix
 * IDs: dtone_123, billers_456, ppn_789, or plain ID for GlobeTopper
 */
const getProductById = async (id) => {
  const idStr = String(id);

  // DT-One product
  if (idStr.startsWith('dtone_')) {
    const realId = idStr.replace('dtone_', '');
    try {
      const p = await dtoneProvider.getProductById(realId);
      const countryIso = p.operator?.country?.iso_code || '';
      const countryName = p.operator?.country?.name || '';
      const opName = p.operator?.name || p.name || '';
      const serviceName = p.service?.name || p.type || 'Topup';

      const destAmount = p.destination?.amount || 0;
      const destCurr = p.destination?.unit || '';
      const sourceAmount = p.source?.amount || 0;
      const sourceCurr = p.source?.unit || '';
      const retailPrice = p.prices?.retail?.amount || sourceAmount;
      const retailCurr = p.prices?.retail?.unit || sourceCurr;
      const curr = destCurr || retailCurr;

      let denomination = '';
      if (destAmount) {
        denomination = `${destAmount} ${destCurr}`;
      } else if (retailPrice) {
        denomination = `${retailPrice} ${retailCurr}`;
      }

      // Determine product characteristics
      const benefitMin = p.benefits?.[0]?.amount?.range?.min || p.source?.amount || 0;
      const benefitMax = p.benefits?.[0]?.amount?.range?.max || p.source?.amount || 0;
      const isRangeProduct = benefitMin !== benefitMax && benefitMax > 0;
      const productType = p.type || '';

      // Collect required fields from product AND service level
      const requiredCreditFields = p.required_credit_party_identifier_fields || p.service?.required_credit_party_identifier_fields || [];
      const requiredSenderFields = p.required_sender_fields || p.service?.required_sender_fields || [];
      const requiredBeneficiaryFields = p.required_beneficiary_fields || p.service?.required_beneficiary_fields || [];
      const requiredDebitFields = p.required_debit_party_identifier_fields || p.service?.required_debit_party_identifier_fields || [];
      const requiredStatementFields = p.required_statement_identifier_fields || p.service?.required_statement_identifier_fields || [];
      const requiredAdditionalFields = p.required_additional_identifier_fields || p.service?.required_additional_identifier_fields || [];

      // Determine credit party type: [[\"mobile_number\"]] → mobile_number
      let creditPartyType = 'mobile_number';
      if (requiredCreditFields.length > 0 && requiredCreditFields[0].length > 0) {
        creditPartyType = requiredCreditFields[0][0];
      }

      return {
        ...p,
        provider: 'dtone',
        providerLabel: 'DT-One',
        id: idStr,
        operatorId: idStr,
        topup_product_id: idStr,
        name: p.name || opName,
        brand: opName,
        description: p.description || p.name || '',
        brand_description: p.description || '',
        country: countryName,
        iso2: countryIso,
        card_image: p.operator?.logo_url || null,
        category: { id: 1001, name: serviceName },
        currency: { code: curr, name: curr },
        usage: productType || 'Digital',
        expiration: '',
        denomination,
        redemptionInfo: denomination,
        redemption_instruction: '',
        term_and_conditions: '',
        priceRange: {
          min: isRangeProduct ? parseFloat(benefitMin) : (parseFloat(retailPrice || destAmount) || 0),
          max: isRangeProduct ? parseFloat(benefitMax) : (parseFloat(retailPrice || destAmount) || 0),
          increment: isRangeProduct ? (p.benefits?.[0]?.amount?.range?.step || 1) : 1,
          isRange: isRangeProduct
        },
        // DT-One specific transaction metadata
        dtoneProductType: productType,
        dtoneIsRanged: isRangeProduct,
        dtoneSourceCurrency: sourceCurr,
        dtoneDestCurrency: destCurr,
        dtoneSourceAmount: sourceAmount,
        dtoneDestAmount: destAmount,
        dtoneRetailPrice: retailPrice,
        dtoneRetailCurrency: retailCurr,
        dtoneCreditPartyType: creditPartyType,
        dtoneRequiredCreditFields: requiredCreditFields,
        dtoneRequiredSenderFields: requiredSenderFields,
        dtoneRequiredBeneficiaryFields: requiredBeneficiaryFields,
        dtoneRequiredDebitFields: requiredDebitFields,
        dtoneRequiredStatementFields: requiredStatementFields,
        dtoneRequiredAdditionalFields: requiredAdditionalFields,
        dtoneService: p.service || null,
        operator: {
          id: idStr,
          name: opName,
          logo_url: p.operator?.logo_url || null,
          country: { iso2: countryIso, name: countryName, currency: { code: curr, name: curr } }
        }
      };
    } catch (err) {
      logger.warn('DT-One getProductById failed', { error: err.message, id: realId });
      return null;
    }
  }

  // Billers product
  if (idStr.startsWith('billers_')) {
    const realId = idStr.replace('billers_', '');
    try {
      // Get biller info from catalog
      const billersData = await billersProvider.getBillers({ BillerID: realId });
      const billers = billersData?.Data || billersData?.Billers || billersData?.data || [];
      const biller = billers.find(b => String(b.BillerID) === String(realId)) || billers[0];

      // Get SKUs for this biller
      let skus = [];
      try {
        const skuData = await billersProvider.getSkus(realId);
        skus = skuData?.Data || skuData?.SKUs || skuData?.data || [];
      } catch (e) {
        logger.warn('Billers getSkus failed', { error: e.message });
      }

      // Fetch input fields for each SKU (parallel), filtered to this biller
      const skusWithInputs = await Promise.all(skus.map(async (sku) => {
        let inputFields = [];
        try {
          const inputData = await billersProvider.getSkuInputs(sku.SKU || sku.SkuCode);
          const allFields = inputData?.Data || inputData?.inputs || [];
          // Filter to only fields matching this biller, or fields with no BillerID
          inputFields = allFields.filter(f =>
            !f.BillerID || String(f.BillerID) === String(realId)
          );
          // Deduplicate by IOID (keep first match per IOID)
          const seen = new Set();
          inputFields = inputFields.filter(f => {
            const key = f.IOID || f.Name || f.name;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
          });
        } catch (e) {
          logger.warn('Billers getSkuInputs failed', { sku: sku.SKU, error: e.message });
        }
        return { ...sku, inputFields };
      }));

      if (!biller) return null;

      const billerCountryCode = biller.CountryCode || '';
      const currencyCode = biller.Currency || COUNTRY_CURRENCY_MAP[billerCountryCode] || '';
      const minAmt = skus.length > 0 ? Math.min(...skus.map(s => parseFloat(s.MinAmount || s.Amount || 0))) : 0;
      const maxAmt = skus.length > 0 ? Math.max(...skus.map(s => parseFloat(s.MaxAmount || s.Amount || 0))) : 0;
      const billerType = biller.BillerType || 'Bill Payment';

      let denomination = '';
      if (skus.length > 0) {
        denomination = skus.map(s => `${s.Description || s.SKU} ${currencyCode}`).join(', ');
      } else if (minAmt && maxAmt) {
        denomination = `${minAmt} - ${maxAmt} ${currencyCode}`;
      } else if (currencyCode) {
        denomination = `Variable (${currencyCode})`;
      }

      const billerCountry = biller.CountryCode || '';
      const billerCountryName = COUNTRY_NAME_MAP[billerCountry] || biller.CountryName || billerCountry;

      // FX rate data — not available yet (dailyfxrate endpoint returns no usable data)
      // USD conversion will be added once AED→USD rate source is confirmed
      const fxRateData = null;

      return {
        ...biller,
        provider: 'billers',
        providerLabel: 'Billers',
        id: idStr,
        operatorId: idStr,
        topup_product_id: idStr,
        billerRealId: realId,
        name: biller.BillerName || '',
        brand: biller.BillerName || '',
        description: biller.Description || biller.BillerName || '',
        brand_description: biller.Description || '',
        country: billerCountryName,
        iso2: billerCountry,
        card_image: biller.Logo || null,
        category: { id: 3001, name: biller.BillerType || 'Bill Payment' },
        currency: { code: currencyCode, name: currencyCode },
        usage: billerType !== 'Bill Payment' ? billerType : '',
        expiration: '',
        denomination,
        redemptionInfo: denomination,
        redemption_instruction: '',
        term_and_conditions: '',
        priceRange: {
          min: minAmt,
          max: maxAmt,
          increment: 1,
          isRange: minAmt !== maxAmt
        },
        skus: skusWithInputs,
        BillerType: billerType,
        BillerSubType: biller.BillerSubType || '',
        BackendFee: biller.BackendFee || 0,
        operator: {
          id: idStr,
          name: biller.BillerName || '',
          logo_url: biller.Logo || null,
          country: { iso2: billerCountry, name: billerCountryName }
        },
        fxRateData,
      };
    } catch (err) {
      logger.warn('Billers getProductById failed', { error: err.message, id: realId });
      return null;
    }
  }

  // PPN product
  if (idStr.startsWith('ppn_')) {
    const realId = idStr.replace('ppn_', '');
    try {
      const data = await ppnProvider.getProductById(realId);
      const products = Array.isArray(data) ? data : (data?.payLoad || data?.skus || data?.data || []);
      const p = products.find(item => String(item.SkuCode || item.skuId) === String(realId)) || products[0];

      if (!p) return null;

      const productName = p.ProductName || p.productName || p.name || '';
      const brandName = p.Brand || p.brand || p.OperatorName || '';
      const currencyCode = p.Currency || p.currency || '';
      const minAmt = p.MinAmount || p.minAmount || 0;
      const maxAmt = p.MaxAmount || p.maxAmount || 0;
      const fixedAmounts = p.FixedAmounts || p.fixedAmounts || [];

      let denomination = '';
      if (fixedAmounts.length > 0) {
        denomination = fixedAmounts.map(a => `${a} ${currencyCode}`).join(', ');
      } else if (minAmt && maxAmt) {
        denomination = `${minAmt} - ${maxAmt} ${currencyCode}`;
      }

      // Build PPN transaction form attributes based on product type
      const ppnCategory = (p.Category || p.category || '').toLowerCase();
      const ppnType = (p.Type || p.type || '').toLowerCase();
      const ppnAttributes = [];
      const isRangePpn = !fixedAmounts.length && minAmt && maxAmt && parseFloat(minAmt) !== parseFloat(maxAmt);

      if (isRangePpn) {
        ppnAttributes.push({ name: 'amount', label: 'Amount', required: true });
      }
      // Topup / Recharge type needs mobile number
      if (ppnCategory.includes('topup') || ppnCategory.includes('recharge') || ppnCategory.includes('mobile') ||
          ppnType.includes('topup') || ppnType.includes('recharge')) {
        ppnAttributes.push({ name: 'mobileNumber', label: 'Mobile Number (with country code)', required: true });
      }
      // Bill Payment / Utility needs account number
      if (ppnCategory.includes('bill') || ppnCategory.includes('utility') || ppnCategory.includes('payment')) {
        ppnAttributes.push({ name: 'accountNumber', label: 'Account / Bill Number', required: true });
      }
      // If no specific field was added, add a generic recipient identifier
      if (!ppnAttributes.find(a => a.name === 'mobileNumber') && !ppnAttributes.find(a => a.name === 'accountNumber')) {
        ppnAttributes.push({ name: 'accountNumber', label: 'Recipient Account / Phone Number', required: true });
      }
      ppnAttributes.push({ name: 'email', label: 'Email Address', required: false });

      // Determine PPN transaction category
      const rawCat = (p.Category || p.category || '').toLowerCase();
      let ppnTxCategory = 'giftcard';
      if (rawCat.includes('pin')) ppnTxCategory = 'pin';
      else if (rawCat.includes('topup') || rawCat.includes('recharge') || rawCat.includes('rtr')) ppnTxCategory = 'rtr';
      else if (rawCat.includes('bill') || rawCat.includes('utility') || rawCat.includes('payment')) ppnTxCategory = 'billpay';
      else if (rawCat.includes('sim') && !rawCat.includes('esim')) ppnTxCategory = 'sim';
      else if (rawCat.includes('esim')) ppnTxCategory = 'esim';
      else if (rawCat.includes('gift')) ppnTxCategory = 'giftcard';

      return {
        ...p,
        provider: 'ppn',
        providerLabel: 'PPN',
        id: idStr,
        operatorId: idStr,
        topup_product_id: idStr,
        ppnSkuId: realId,
        ppnCountryCode: p.CountryCode || p.countryCode || '',
        ppnCategory: ppnTxCategory,
        name: productName,
        brand: brandName || productName,
        description: p.Description || p.description || productName,
        brand_description: p.Description || p.description || '',
        country: p.Country || p.country || '',
        iso2: p.CountryCode || p.countryCode || '',
        card_image: p.LogoUrl || p.logoUrl || null,
        category: { id: 2001, name: p.Category || p.category || 'Topup' },
        currency: { code: currencyCode, name: currencyCode },
        usage: p.Type || p.type || 'Digital',
        expiration: '',
        denomination,
        redemptionInfo: denomination,
        redemption_instruction: '',
        term_and_conditions: '',
        priceRange: {
          min: parseFloat(minAmt) || 0,
          max: parseFloat(maxAmt) || 0,
          increment: 1,
          isRange: isRangePpn
        },
        operator: {
          id: idStr,
          name: brandName || productName,
          logo_url: p.LogoUrl || p.logoUrl || null,
          country: { iso2: p.CountryCode || '', name: p.Country || '' }
        }
      };
    } catch (err) {
      logger.warn('PPN getProductById failed', { error: err.message, id: realId });
      return null;
    }
  }

  // Default: GlobeTopper (no prefix)
  // The listing uses topup_product_id from catalogue, so we must search both
  try {
    const [catalogue, products] = await Promise.all([
      globetopperProvider.getCatalogue({}),
      globetopperProvider.getProducts({})
    ]);

    // Search by: topup_product_id (catalogue), BillerID, or operator.id
    const catItem = catalogue.find(c => c.topup_product_id == id);
    let product = products.find(p =>
      p.BillerID == id || (p.operator && p.operator.id == id)
    );

    // If found via catalogue, match the corresponding product
    if (catItem && !product) {
      product = products.find(p => p.operator && p.operator.id == catItem.topup_product_id);
    }

    const merged = { ...catItem, ...product };

    if (catItem || product) {
      const denomStr = catItem?.denomination || merged.denomination || '';
      // Parse min/max from denomination string like "2.00 - 500.00 by 1.00"
      const denomMatch = denomStr.match(/([\d.,]+)\s*-\s*([\d.,]+)/);
      const parsedMin = denomMatch ? parseFloat(denomMatch[1].replace(/,/g, '')) : 0;
      const parsedMax = denomMatch ? parseFloat(denomMatch[2].replace(/,/g, '')) : 0;

      // Build attributes — use product's request_attributes, or create amount field from denomination
      let attrs = merged.request_attributes?.map(attr => ({
        name: attr.name,
        label: attr.label,
        required: attr.required
      })) || [];
      if (attrs.length === 0) {
        attrs = [
          ...(parsedMin && parsedMax ? [{ name: 'amount', label: 'Amount', required: true }] : []),
          { name: 'email', label: 'Email Address', required: true },
          { name: 'first_name', label: 'First Name', required: true },
          { name: 'last_name', label: 'Last Name', required: true },
          { name: 'notif_tele', label: 'Phone Number', required: true },
        ];
      }
      // Ensure phone field is always present for GlobeTopper
      if (!attrs.find(a => a.name === 'notif_tele' || a.name === 'phoneNumber' || a.name === 'phone')) {
        attrs.push({ name: 'notif_tele', label: 'Phone Number', required: true });
      }

      return {
        ...merged,
        provider: 'globetopper',
        providerLabel: 'GlobeTopper',
        id: merged.BillerID || id,
        operatorId: merged.operator?.id || catItem?.topup_product_id || id,
        topup_product_id: catItem?.topup_product_id || merged.operator?.id || id,
        name: merged.name || merged.brand || '',
        brand: merged.brand || merged.name || '',
        description: merged.description || merged.brand_description || '',
        brand_description: merged.brand_description || '',
        country: merged.country || merged.operator?.country?.name || '',
        iso2: merged.iso2 || merged.operator?.country?.iso2 || '',
        card_image: merged.card_image || merged.operator?.logo_url || null,
        category: merged.category || { id: 9999, name: 'Gift Card' },
        currency: merged.currency || { code: merged.operator?.country?.currency?.code || '', name: merged.operator?.country?.currency?.name || '' },
        usage: merged.usage || '',
        expiration: merged.expiration || '',
        denomination: denomStr,
        redemptionInfo: merged.additional_details?.find(d => d.value)?.value || denomStr,
        redemption_instruction: merged.redemption_instruction || '',
        term_and_conditions: merged.term_and_conditions || '',
        priceRange: {
          min: parseFloat(merged.min || 0) || parsedMin,
          max: parseFloat(String(merged.max || '0').replace(/,/g, '')) || parsedMax,
          increment: parseFloat(merged.increment || 0),
          isRange: merged.is_a_range || (parsedMin !== parsedMax && parsedMax > 0)
        },
        attributes: attrs,
      };
    }
  } catch (err) {
    logger.warn('GlobeTopper getProductById failed', { error: err.message });
  }

  return null;
};

module.exports = { getAllProducts, getProductById };
