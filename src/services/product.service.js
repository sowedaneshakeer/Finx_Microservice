const fs = require('fs');
const path = require('path');
const globetopperProvider = require('../providers/globetopper.provider');
const dtoneProvider = require('../providers/dtone.provider');
const ppnProvider = require('../providers/ppn.provider');
const billersProvider = require('../providers/billers.provider');
const logger = require('../utils/logger');
const { encodeProductId, decodeProductId, PROVIDER_TO_CODE } = require('../utils/providerCodes');

// ─── Disk-Persisted Cache ────────────────────────────────────────────────────
const CACHE_DIR = path.join(__dirname, '../../cache');
const CACHE_FILE = path.join(CACHE_DIR, 'products_cache.json');

/**
 * Migrate cached products to use new numeric-code IDs (e.g. billers_xxx → 1003_xxx).
 * This ensures disk cache saved by old code is automatically upgraded on load.
 */
function migrateCacheProducts(products) {
  let migrated = 0;
  const result = products.map(p => {
    const billerID = p.BillerID || '';
    // Check if BillerID uses an old provider-name prefix (e.g. billers_, dtone_, ppn_)
    const encoded = encodeProductId(billerID);
    if (encoded !== billerID) {
      migrated++;
      const updated = { ...p, BillerID: encoded };
      if (updated.topup_product_id) updated.topup_product_id = encodeProductId(updated.topup_product_id);
      if (updated.operator && updated.operator.id) {
        updated.operator = { ...updated.operator, id: encoded };
      }
      return updated;
    }
    return p;
  });
  if (migrated > 0) {
    logger.info(`Migrated ${migrated} products from old ID format to numeric codes`);
  }
  return result;
}

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
          products: migrateCacheProducts(saved[provider].products),
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

  return allProducts.map(p => {
    const rawId = String(p.topup_product_id || p.operator?.id || p.sku || p.id || p.BillerID || '');
    const prefixedId = PROVIDER_TO_CODE['globetopper'] + '_' + rawId;
    return {
      ...p,
      provider: 'globetopper',
      providerLabel: 'GlobeTopper',
      productId: rawId,
      productName: p.name || '',
      topup_product_id: prefixedId,
      BillerID: prefixedId,
    };
  });
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
    const prefixedId = PROVIDER_TO_CODE['dtone'] + '_' + (p.id || '');

    // DT-One: amounts can be numbers (fixed) or objects {min, max} (ranged)
    const rawDestAmt = p.destination?.amount;
    const destCurr = p.destination?.unit || '';
    const rawSrcAmt = p.source?.amount;
    const sourceCurr = p.source?.unit || '';
    const rawRetailAmt = p.prices?.retail?.amount;
    const retailCurr = p.prices?.retail?.unit || sourceCurr;

    const isRanged = (typeof rawDestAmt === 'object' && rawDestAmt !== null && rawDestAmt.min !== undefined)
      || (typeof rawSrcAmt === 'object' && rawSrcAmt !== null && rawSrcAmt.min !== undefined)
      || (p.type || '').startsWith('RANGED_VALUE');

    let destAmount, sourceAmount, retailPrice;
    if (isRanged) {
      const destRange = (typeof rawDestAmt === 'object' && rawDestAmt !== null) ? rawDestAmt : null;
      const srcRange = (typeof rawSrcAmt === 'object' && rawSrcAmt !== null) ? rawSrcAmt : null;
      destAmount = destRange ? `${destRange.min}-${destRange.max}` : (typeof rawDestAmt === 'number' ? rawDestAmt : 0);
      sourceAmount = srcRange ? srcRange.min : (typeof rawSrcAmt === 'number' ? rawSrcAmt : 0);
      retailPrice = (typeof rawRetailAmt === 'object' && rawRetailAmt !== null) ? rawRetailAmt.min : (typeof rawRetailAmt === 'number' ? rawRetailAmt : sourceAmount);
    } else {
      destAmount = (typeof rawDestAmt === 'number') ? rawDestAmt : 0;
      sourceAmount = (typeof rawSrcAmt === 'number') ? rawSrcAmt : 0;
      retailPrice = (typeof rawRetailAmt === 'number') ? rawRetailAmt : sourceAmount;
    }

    // Build denomination string
    let denomination = '';
    if (isRanged) {
      const dRange = (typeof rawDestAmt === 'object' && rawDestAmt !== null) ? rawDestAmt : null;
      if (dRange) {
        denomination = `${dRange.min} - ${dRange.max} ${destCurr}`;
      } else {
        const sRange = (typeof rawSrcAmt === 'object' && rawSrcAmt !== null) ? rawSrcAmt : null;
        denomination = sRange ? `${sRange.min} - ${sRange.max} ${sourceCurr}` : '';
      }
    } else if (destAmount) {
      denomination = `${destAmount} ${destCurr}`;
    } else if (retailPrice) {
      denomination = `${retailPrice} ${retailCurr}`;
    }

    const minDisplay = isRanged ? String(retailPrice || sourceAmount) : String(retailPrice || destAmount);
    const maxDisplay = isRanged
      ? String((typeof rawRetailAmt === 'object' && rawRetailAmt !== null) ? rawRetailAmt.max : (typeof rawSrcAmt === 'object' && rawSrcAmt !== null) ? rawSrcAmt.max : retailPrice || destAmount)
      : String(retailPrice || destAmount);

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
      min: minDisplay,
      max: maxDisplay,
      denominations: (!isRanged && destAmount) ? [destAmount] : [],
      denomination,
      user_display: denomination,
      card_image: p.operator?.logo_url || null,
      imageUrl: p.operator?.logo_url || null,
      dtoneIsRanged: isRanged,
    };
  });
}

/**
 * Normalize PPN product to orchestrator format
 *
 * PPN API returns min/max as objects:
 *   { faceValue, faceValueCurrency, deliveredAmount, deliveryCurrencyCode, cost, costCurrency, faceValueInWalletCurrency }
 * category is a plain string, imageUrl is the logo field, and there is no country name (only countryCode).
 */
function normalizePpn(data) {
  const products = Array.isArray(data) ? data : (data?.payLoad || data?.skus || data?.data || []);

  return products.map(p => {
    const skuCode = p.SkuCode || p.skuId || p.sku_code || p.id || '';
    const prefixedId = PROVIDER_TO_CODE['ppn'] + '_' + skuCode;
    const productName = p.ProductName || p.productName || p.product_name || p.name || '';
    const brandName = p.Brand || p.brand || p.OperatorName || p.operatorName || '';
    const countryCode = p.CountryCode || p.countryCode || p.country_code || '';
    const countryName = p.Country || p.country || COUNTRY_NAME_MAP[countryCode] || countryCode;

    // PPN min/max can be objects with { faceValue, faceValueCurrency, ... } or simple numbers
    const minObj = p.MinAmount || p.minAmount || p.min || 0;
    const maxObj = p.MaxAmount || p.maxAmount || p.max || 0;
    const minAmt = (typeof minObj === 'object' && minObj !== null) ? (minObj.faceValue || minObj.deliveredAmount || 0) : Number(minObj) || 0;
    const maxAmt = (typeof maxObj === 'object' && maxObj !== null) ? (maxObj.faceValue || maxObj.deliveredAmount || 0) : Number(maxObj) || 0;

    // Extract currency from min/max objects or from explicit fields
    const currencyFromMin = (typeof minObj === 'object' && minObj !== null) ? (minObj.faceValueCurrency || minObj.deliveryCurrencyCode || '') : '';
    const currencyCode = p.Currency || p.currency || currencyFromMin || COUNTRY_CURRENCY_MAP[countryCode] || '';

    // category can be a plain string from PPN API
    const rawCategory = p.Category || p.category || 'Topup';
    const categoryName = (typeof rawCategory === 'string') ? rawCategory : (rawCategory?.name || 'Topup');
    const typeName = p.Type || p.type || categoryName;

    // PPN uses imageUrl for logos
    const logoUrl = p.LogoUrl || p.logoUrl || p.logo_url || p.ImageUrl || p.imageUrl || null;

    const fixedAmounts = p.FixedAmounts || p.fixedAmounts || p.denominations || [];

    let denomination = '';
    if (fixedAmounts.length > 0) {
      denomination = fixedAmounts.map(a => `${a} ${currencyCode}`).join(', ');
    } else if (minAmt && maxAmt && minAmt !== maxAmt) {
      denomination = `${minAmt} - ${maxAmt} ${currencyCode}`;
    } else if (minAmt) {
      denomination = `${minAmt} ${currencyCode}`;
    }

    // Use skuName as fallback for denomination (e.g. "AT&T PIN US 15.00 USD")
    if (!denomination && p.skuName) {
      denomination = p.skuName;
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
    const prefixedId = PROVIDER_TO_CODE['billers'] + '_' + billerId;
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
      // Ensure existing cache uses new ID format
      productCache.dtone.products = migrateCacheProducts(productCache.dtone.products);
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
      // Ensure existing cache uses new ID format
      productCache.billers.products = migrateCacheProducts(productCache.billers.products);
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
 * Accepts numeric codes (1002_123) or old prefixes (dtone_123) or plain IDs (GlobeTopper)
 */
const getProductById = async (id) => {
  const idStr = String(id);
  const decoded = decodeProductId(idStr);
  const encodedId = encodeProductId(idStr);

  // DT-One product
  if (decoded.provider === 'dtone') {
    const realId = decoded.rawId;
    try {
      const p = await dtoneProvider.getProductById(realId);
      const countryIso = p.operator?.country?.iso_code || '';
      const countryName = p.operator?.country?.name || '';
      const opName = p.operator?.name || p.name || '';
      const serviceName = p.service?.name || p.type || 'Topup';

      // DT-One: amounts can be simple numbers (fixed) or objects {min, max} (ranged)
      const rawDestAmt = p.destination?.amount;
      const destCurr = p.destination?.unit || '';
      const rawSrcAmt = p.source?.amount;
      const sourceCurr = p.source?.unit || '';
      const rawRetailAmt = p.prices?.retail?.amount;
      const retailCurr = p.prices?.retail?.unit || sourceCurr;

      // Detect ranged products: amount fields are objects with min/max
      const isRangeProduct = (typeof rawDestAmt === 'object' && rawDestAmt !== null && rawDestAmt.min !== undefined)
        || (typeof rawSrcAmt === 'object' && rawSrcAmt !== null && rawSrcAmt.min !== undefined)
        || (p.type || '').startsWith('RANGED_VALUE');
      const productType = p.type || '';

      // Extract numeric amounts (for fixed products) or min/max (for ranged)
      let destAmount, sourceAmount, retailPrice;
      let rangeMin = 0, rangeMax = 0, rangeIncrement = 1;

      if (isRangeProduct) {
        // Ranged: extract min/max from destination or source
        const destRange = (typeof rawDestAmt === 'object' && rawDestAmt !== null) ? rawDestAmt : null;
        const srcRange = (typeof rawSrcAmt === 'object' && rawSrcAmt !== null) ? rawSrcAmt : null;
        const retailRange = (typeof rawRetailAmt === 'object' && rawRetailAmt !== null) ? rawRetailAmt : null;
        destAmount = destRange ? destRange.min : (typeof rawDestAmt === 'number' ? rawDestAmt : 0);
        sourceAmount = srcRange ? srcRange.min : (typeof rawSrcAmt === 'number' ? rawSrcAmt : 0);
        retailPrice = retailRange ? retailRange.min : (typeof rawRetailAmt === 'number' ? rawRetailAmt : sourceAmount);
        // Use source range for price range (what user pays)
        if (srcRange) {
          rangeMin = srcRange.min || 0;
          rangeMax = srcRange.max || 0;
          rangeIncrement = srcRange.increment || destRange?.increment || 1;
        } else if (destRange) {
          rangeMin = destRange.min || 0;
          rangeMax = destRange.max || 0;
          rangeIncrement = destRange.increment || 1;
        }
      } else {
        // Fixed: amounts are simple numbers
        destAmount = (typeof rawDestAmt === 'number') ? rawDestAmt : 0;
        sourceAmount = (typeof rawSrcAmt === 'number') ? rawSrcAmt : 0;
        retailPrice = (typeof rawRetailAmt === 'number') ? rawRetailAmt : sourceAmount;
      }

      const curr = destCurr || retailCurr;

      let denomination = '';
      if (isRangeProduct) {
        const dRange = (typeof rawDestAmt === 'object' && rawDestAmt !== null) ? rawDestAmt : null;
        if (dRange) {
          denomination = `${dRange.min} - ${dRange.max} ${destCurr}`;
        } else {
          denomination = `${rangeMin} - ${rangeMax} ${sourceCurr}`;
        }
      } else if (destAmount) {
        denomination = `${destAmount} ${destCurr}`;
      } else if (retailPrice) {
        denomination = `${retailPrice} ${retailCurr}`;
      }

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
        id: encodedId,
        operatorId: encodedId,
        topup_product_id: encodedId,
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
          min: isRangeProduct ? rangeMin : (parseFloat(retailPrice || destAmount) || 0),
          max: isRangeProduct ? rangeMax : (parseFloat(retailPrice || destAmount) || 0),
          increment: isRangeProduct ? rangeIncrement : 1,
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
          id: encodedId,
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
  if (decoded.provider === 'billers') {
    const realId = decoded.rawId;
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
        id: encodedId,
        operatorId: encodedId,
        topup_product_id: encodedId,
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
          id: encodedId,
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
  if (decoded.provider === 'ppn') {
    const realId = decoded.rawId;
    try {
      const data = await ppnProvider.getProductById(realId);
      const products = Array.isArray(data) ? data : (data?.payLoad || data?.skus || data?.data || []);
      const p = products.find(item => String(item.SkuCode || item.skuId) === String(realId)) || products[0];

      if (!p) return null;

      const productName = p.ProductName || p.productName || p.name || '';
      const brandName = p.Brand || p.brand || p.OperatorName || p.operatorName || '';

      // PPN min/max can be objects with { faceValue, faceValueCurrency, ... }
      const minObj = p.MinAmount || p.minAmount || p.min || 0;
      const maxObj = p.MaxAmount || p.maxAmount || p.max || 0;
      const minAmt = (typeof minObj === 'object' && minObj !== null) ? (minObj.faceValue || minObj.deliveredAmount || 0) : Number(minObj) || 0;
      const maxAmt = (typeof maxObj === 'object' && maxObj !== null) ? (maxObj.faceValue || maxObj.deliveredAmount || 0) : Number(maxObj) || 0;

      const currencyFromMin = (typeof minObj === 'object' && minObj !== null) ? (minObj.faceValueCurrency || minObj.deliveryCurrencyCode || '') : '';
      const ppnCountryCode = p.CountryCode || p.countryCode || '';
      const currencyCode = p.Currency || p.currency || currencyFromMin || COUNTRY_CURRENCY_MAP[ppnCountryCode] || '';
      const countryName = p.Country || p.country || COUNTRY_NAME_MAP[ppnCountryCode] || ppnCountryCode;

      const fixedAmounts = p.FixedAmounts || p.fixedAmounts || [];
      const logoUrl = p.LogoUrl || p.logoUrl || p.logo_url || p.ImageUrl || p.imageUrl || null;

      let denomination = '';
      if (fixedAmounts.length > 0) {
        denomination = fixedAmounts.map(a => `${a} ${currencyCode}`).join(', ');
      } else if (minAmt && maxAmt && minAmt !== maxAmt) {
        denomination = `${minAmt} - ${maxAmt} ${currencyCode}`;
      } else if (minAmt) {
        denomination = `${minAmt} ${currencyCode}`;
      }
      if (!denomination && p.skuName) {
        denomination = p.skuName;
      }

      // Build PPN transaction form attributes based on product type
      const rawCategory = p.Category || p.category || 'Topup';
      const ppnCategory = (typeof rawCategory === 'string' ? rawCategory : (rawCategory?.name || '')).toLowerCase();
      const ppnType = (p.Type || p.type || '').toLowerCase();
      const ppnAttributes = [];
      const isRangePpn = !fixedAmounts.length && minAmt && maxAmt && minAmt !== maxAmt;

      if (isRangePpn) {
        ppnAttributes.push({ name: 'amount', label: 'Amount', required: true });
      }
      // Topup / Recharge type needs mobile number
      if (ppnCategory.includes('topup') || ppnCategory.includes('recharge') || ppnCategory.includes('mobile') ||
          ppnType.includes('topup') || ppnType.includes('recharge') || ppnCategory.includes('rtr')) {
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
      let ppnTxCategory = 'giftcard';
      if (ppnCategory.includes('pin')) ppnTxCategory = 'pin';
      else if (ppnCategory.includes('topup') || ppnCategory.includes('recharge') || ppnCategory.includes('rtr')) ppnTxCategory = 'rtr';
      else if (ppnCategory.includes('bill') || ppnCategory.includes('utility') || ppnCategory.includes('payment')) ppnTxCategory = 'billpay';
      else if (ppnCategory.includes('sim') && !ppnCategory.includes('esim')) ppnTxCategory = 'sim';
      else if (ppnCategory.includes('esim')) ppnTxCategory = 'esim';
      else if (ppnCategory.includes('gift')) ppnTxCategory = 'giftcard';

      const categoryName = typeof rawCategory === 'string' ? rawCategory : (rawCategory?.name || 'Topup');

      // Extract face value / delivered amount details from min object
      const faceValue = (typeof minObj === 'object' && minObj !== null) ? (minObj.faceValue || 0) : Number(minObj) || 0;
      const faceValueCurrency = (typeof minObj === 'object' && minObj !== null) ? (minObj.faceValueCurrency || '') : currencyCode;
      const deliveredAmount = (typeof minObj === 'object' && minObj !== null) ? (minObj.deliveredAmount || 0) : 0;
      const deliveryCurrencyCode = (typeof minObj === 'object' && minObj !== null) ? (minObj.deliveryCurrencyCode || '') : '';

      return {
        ...p,
        provider: 'ppn',
        providerLabel: 'PPN',
        id: encodedId,
        operatorId: encodedId,
        topup_product_id: encodedId,
        ppnSkuId: realId,
        ppnCountryCode,
        ppnCategory: ppnTxCategory,
        name: productName,
        brand: brandName || productName,
        description: p.productDescription || p.Description || p.description || p.additionalInformation || productName,
        brand_description: p.productDescription || p.Description || p.description || '',
        country: countryName,
        iso2: ppnCountryCode,
        card_image: logoUrl,
        category: { id: 2001, name: categoryName },
        currency: { code: currencyCode, name: currencyCode },
        usage: p.Type || p.type || p.deliveryAmountType || 'Digital',
        expiration: p.validity || '',
        denomination,
        redemptionInfo: denomination,
        redemption_instruction: p.additionalInformation || '',
        term_and_conditions: '',
        min: String(minAmt),
        max: String(maxAmt),
        priceRange: {
          min: minAmt,
          max: maxAmt,
          increment: p.allowDecimal ? 0.01 : 1,
          isRange: isRangePpn
        },
        operator: {
          id: encodedId,
          name: brandName || productName,
          logo_url: logoUrl,
          country: { iso2: ppnCountryCode, name: countryName, currency: { code: currencyCode, name: currencyCode } }
        },
        // PPN-specific detail fields
        ppnDetails: {
          skuName: p.skuName || '',
          productId: p.productId || '',
          operatorId: p.operatorId || '',
          faceValue,
          faceValueCurrency,
          deliveredAmount,
          deliveryCurrencyCode,
          exchangeRate: p.exchangeRate || 0,
          fee: p.fee || 0,
          salesTax: p.salesTax || 0,
          isSalesTaxCharged: p.isSalesTaxCharged || false,
          benefitType: p.benefitType || '',
          deliveryAmountType: p.deliveryAmountType || '',
          allowDecimal: p.allowDecimal !== false,
          localPhoneNumberLength: p.localPhoneNumberLength || 0,
          internationalCountryCode: p.internationalCountryCode || [],
          additionalInformation: p.additionalInformation || '',
          productDescription: p.productDescription || '',
          validity: p.validity || '',
          subCategory: p.subCategory || '',
          requireFetchBundle: p.requireFetchBundle || false,
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
    const gtRawId = decoded.rawId;
    const [catalogue, products] = await Promise.all([
      globetopperProvider.getCatalogue({}),
      globetopperProvider.getProducts({})
    ]);

    // Search by: topup_product_id (catalogue), BillerID, or operator.id
    const catItem = catalogue.find(c => c.topup_product_id == gtRawId);
    let product = products.find(p =>
      p.BillerID == gtRawId || (p.operator && p.operator.id == gtRawId)
    );

    // If found via catalogue, match the corresponding product
    if (catItem && !product) {
      product = products.find(p => p.operator && p.operator.id == gtRawId);
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
        id: encodedId,
        operatorId: encodedId,
        topup_product_id: encodedId,
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

// ISO3 → ISO2 mapping for country code normalization
const ISO3_TO_ISO2 = {
  AFG:'AF',ALB:'AL',DZA:'DZ',ASM:'AS',AND:'AD',AGO:'AO',AIA:'AI',ATG:'AG',ARG:'AR',ARM:'AM',
  ABW:'AW',AUS:'AU',AUT:'AT',AZE:'AZ',BHS:'BS',BHR:'BH',BGD:'BD',BRB:'BB',BLR:'BY',BEL:'BE',
  BLZ:'BZ',BEN:'BJ',BMU:'BM',BTN:'BT',BOL:'BO',BIH:'BA',BWA:'BW',BRA:'BR',BRN:'BN',BGR:'BG',
  BFA:'BF',BDI:'BI',KHM:'KH',CMR:'CM',CAN:'CA',CPV:'CV',CYM:'KY',CAF:'CF',TCD:'TD',CHL:'CL',
  CHN:'CN',COL:'CO',COM:'KM',COG:'CG',COD:'CD',COK:'CK',CRI:'CR',CIV:'CI',HRV:'HR',CUB:'CU',
  CUW:'CW',CYP:'CY',CZE:'CZ',DNK:'DK',DJI:'DJ',DMA:'DM',DOM:'DO',ECU:'EC',EGY:'EG',SLV:'SV',
  GNQ:'GQ',ERI:'ER',EST:'EE',SWZ:'SZ',ETH:'ET',FJI:'FJ',FIN:'FI',FRA:'FR',GUF:'GF',PYF:'PF',
  GAB:'GA',GMB:'GM',GEO:'GE',DEU:'DE',GHA:'GH',GIB:'GI',GRC:'GR',GRD:'GD',GLP:'GP',GUM:'GU',
  GTM:'GT',GIN:'GN',GNB:'GW',GUY:'GY',HTI:'HT',HND:'HN',HKG:'HK',HUN:'HU',ISL:'IS',IND:'IN',
  IDN:'ID',IRN:'IR',IRQ:'IQ',IRL:'IE',ISR:'IL',ITA:'IT',JAM:'JM',JPN:'JP',JOR:'JO',KAZ:'KZ',
  KEN:'KE',KIR:'KI',PRK:'KP',KOR:'KR',KWT:'KW',KGZ:'KG',LAO:'LA',LVA:'LV',LBN:'LB',LSO:'LS',
  LBR:'LR',LBY:'LY',LIE:'LI',LTU:'LT',LUX:'LU',MAC:'MO',MDG:'MG',MWI:'MW',MYS:'MY',MDV:'MV',
  MLI:'ML',MLT:'MT',MHL:'MH',MTQ:'MQ',MRT:'MR',MUS:'MU',MYT:'YT',MEX:'MX',FSM:'FM',MDA:'MD',
  MCO:'MC',MNG:'MN',MNE:'ME',MSR:'MS',MAR:'MA',MOZ:'MZ',MMR:'MM',NAM:'NA',NRU:'NR',NPL:'NP',
  NLD:'NL',NCL:'NC',NZL:'NZ',NIC:'NI',NER:'NE',NGA:'NG',NIU:'NU',NFK:'NF',MKD:'MK',MNP:'MP',
  NOR:'NO',OMN:'OM',PAK:'PK',PLW:'PW',PSE:'PS',PAN:'PA',PNG:'PG',PRY:'PY',PER:'PE',PHL:'PH',
  POL:'PL',PRT:'PT',PRI:'PR',QAT:'QA',REU:'RE',ROU:'RO',RUS:'RU',RWA:'RW',KNA:'KN',LCA:'LC',
  VCT:'VC',WSM:'WS',SMR:'SM',STP:'ST',SAU:'SA',SEN:'SN',SRB:'RS',SYC:'SC',SLE:'SL',SGP:'SG',
  SXM:'SX',SVK:'SK',SVN:'SI',SLB:'SB',SOM:'SO',ZAF:'ZA',ESP:'ES',LKA:'LK',SDN:'SD',SUR:'SR',
  SWE:'SE',CHE:'CH',SYR:'SY',TWN:'TW',TJK:'TJ',TZA:'TZ',THA:'TH',TLS:'TL',TGO:'TG',TON:'TO',
  TTO:'TT',TUN:'TN',TUR:'TR',TKM:'TM',TCA:'TC',TUV:'TV',UGA:'UG',UKR:'UA',ARE:'AE',GBR:'GB',
  USA:'US',URY:'UY',UZB:'UZ',VUT:'VU',VEN:'VE',VNM:'VN',VGB:'VG',VIR:'VI',YEM:'YE',ZMB:'ZM',
  ZWE:'ZW',XKX:'XK',SSD:'SS',BES:'BQ',MAF:'MF',BLM:'BL',SPM:'PM',WLF:'WF',ESH:'EH',
};

/**
 * Get unified country list from ALL provider product caches.
 * Normalizes ISO3 → ISO2, deduplicates, and returns sorted list.
 */
const getUnifiedCountries = () => {
  const countryMap = new Map(); // keyed by iso2

  for (const provider of ['globetopper', 'dtone', 'ppn', 'billers']) {
    const products = productCache[provider].products || [];
    for (const p of products) {
      let rawIso = p.iso2 || p.countryCode || (p.operator?.country?.iso2) || '';
      if (!rawIso) continue;

      rawIso = rawIso.toUpperCase().trim();
      if (rawIso === 'WW' || rawIso === 'WWX' || rawIso === 'XX') continue;

      // Normalize: convert ISO3 to ISO2
      const iso2 = rawIso.length === 3 ? (ISO3_TO_ISO2[rawIso] || rawIso) : rawIso;
      const name = p.country || p.operator?.country?.name || '';

      if (countryMap.has(iso2)) {
        const existing = countryMap.get(iso2);
        existing.productCount++;
        // Prefer longer/more descriptive name (not just the ISO code)
        if (name.length > 2 && name.length > existing.name.length) {
          existing.name = name;
        }
      } else {
        countryMap.set(iso2, { iso2, name: name.length > 2 ? name : iso2, productCount: 1 });
      }
    }
  }

  return Array.from(countryMap.values()).sort((a, b) => a.name.localeCompare(b.name));
};

module.exports = { getAllProducts, getProductById, getUnifiedCountries };
