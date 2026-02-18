const globetopper = require('../../providers/globetopper.provider');

const getCatalogue = async (filters) => {
  // Non-GlobeTopper products don't have catalogue data - return empty
  if (filters.productId && /^(dtone|billers|ppn)_/.test(filters.productId)) {
    return [];
  }
  return await globetopper.getCatalogue(filters);
};

module.exports = { getCatalogue };
