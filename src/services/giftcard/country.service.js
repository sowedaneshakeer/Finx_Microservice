const { getUnifiedCountries } = require('../product.service');
const globetopper = require('../../providers/globetopper.provider');
const logger = require('../../utils/logger');

/**
 * Get all countries — unified across all providers (GlobeTopper, DT-One, Billers, PPN).
 * Extracts from product cache so it reflects actual available products.
 */
const getAllCountries = async () => {
  const unified = getUnifiedCountries();

  // If product cache is empty (first startup before warm-up), fall back to GlobeTopper
  if (unified.length === 0) {
    logger.warn('Unified country list empty (cache not ready), falling back to GlobeTopper');
    return await globetopper.getCountries();
  }

  return unified;
};

const getCountryByIso = async (iso) => {
  const countries = await getAllCountries();
  return countries.find(c => c.iso2 === iso.toUpperCase());
};

module.exports = { getAllCountries, getCountryByIso };
