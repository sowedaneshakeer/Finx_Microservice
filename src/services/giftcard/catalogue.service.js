const globetopper = require('../../providers/globetopper.provider');
const { decodeProductId } = require('../../utils/providerCodes');

const getCatalogue = async (filters) => {
  // Non-GlobeTopper products don't have catalogue data - return empty
  if (filters.productId) {
    const { provider } = decodeProductId(filters.productId);
    if (provider !== 'globetopper') return [];
  }
  return await globetopper.getCatalogue(filters);
};

module.exports = { getCatalogue };
