/**
 * Provider Code Mapping
 *
 * Each provider is assigned a numeric code to hide provider names
 * from external users in product IDs and API responses.
 *
 * Product IDs: {providerCode}_{productId}  e.g. 1004_1372
 * Internally:  {providerName}_{productId}  e.g. ppn_1372
 */

const CODE_TO_PROVIDER = {
  '1001': 'globetopper',
  '1002': 'dtone',
  '1003': 'billers',
  '1004': 'ppn',
};

const PROVIDER_TO_CODE = {};
for (const [code, name] of Object.entries(CODE_TO_PROVIDER)) {
  PROVIDER_TO_CODE[name] = code;
}

// Old prefix patterns for backward compatibility
const OLD_PREFIX_TO_PROVIDER = {
  'dtone': 'dtone',
  'billers': 'billers',
  'ppn': 'ppn',
};

/**
 * Encode a provider-prefixed product ID to use numeric code.
 * e.g. 'ppn_1372' → '1004_1372'
 *      'dtone_5000' → '1002_5000'
 *      'billers_ABC' → '1003_ABC'
 *      '12345' (globetopper, no prefix) → '1001_12345'
 *      Already encoded '1004_1372' → '1004_1372' (no double-encode)
 */
function encodeProductId(rawId) {
  if (!rawId) return rawId;
  const idStr = String(rawId);

  // Already encoded (starts with a known code)?
  const underscoreIdx = idStr.indexOf('_');
  if (underscoreIdx > 0) {
    const prefix = idStr.substring(0, underscoreIdx);
    if (CODE_TO_PROVIDER[prefix]) return idStr; // already encoded

    // Has old provider prefix (ppn_, dtone_, billers_)
    if (OLD_PREFIX_TO_PROVIDER[prefix]) {
      const code = PROVIDER_TO_CODE[prefix];
      const realId = idStr.substring(underscoreIdx + 1);
      return `${code}_${realId}`;
    }
  }

  // No prefix = GlobeTopper
  return `${PROVIDER_TO_CODE['globetopper']}_${idStr}`;
}

/**
 * Decode a numeric-coded product ID back to provider-prefixed format.
 * e.g. '1004_1372' → { provider: 'ppn', rawId: '1372', internalId: 'ppn_1372' }
 *      '1001_12345' → { provider: 'globetopper', rawId: '12345', internalId: '12345' }
 *
 * Also accepts old format for backward compatibility:
 *      'ppn_1372' → { provider: 'ppn', rawId: '1372', internalId: 'ppn_1372' }
 *      '12345' → { provider: 'globetopper', rawId: '12345', internalId: '12345' }
 */
function decodeProductId(encodedId) {
  if (!encodedId) return { provider: 'globetopper', rawId: encodedId, internalId: encodedId };
  const idStr = String(encodedId);

  const underscoreIdx = idStr.indexOf('_');
  if (underscoreIdx > 0) {
    const prefix = idStr.substring(0, underscoreIdx);
    const rawId = idStr.substring(underscoreIdx + 1);

    // Numeric code prefix
    if (CODE_TO_PROVIDER[prefix]) {
      const provider = CODE_TO_PROVIDER[prefix];
      // GlobeTopper uses plain ID internally (no prefix)
      const internalId = provider === 'globetopper' ? rawId : `${provider}_${rawId}`;
      return { provider, rawId, internalId };
    }

    // Old provider name prefix (backward compat)
    if (OLD_PREFIX_TO_PROVIDER[prefix]) {
      return { provider: prefix, rawId, internalId: idStr };
    }
  }

  // No prefix = GlobeTopper
  return { provider: 'globetopper', rawId: idStr, internalId: idStr };
}

/**
 * Get the numeric code for a provider name
 */
function getProviderCode(providerName) {
  return PROVIDER_TO_CODE[providerName] || PROVIDER_TO_CODE['globetopper'];
}

/**
 * Get the provider name from a numeric code
 */
function getProviderName(code) {
  return CODE_TO_PROVIDER[code] || 'globetopper';
}

module.exports = {
  encodeProductId,
  decodeProductId,
  getProviderCode,
  getProviderName,
  CODE_TO_PROVIDER,
  PROVIDER_TO_CODE,
};
