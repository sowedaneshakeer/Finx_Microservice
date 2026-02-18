const globetopper = require('../../providers/globetopper.provider');

const getAllCurrencies = async () => {
  return await globetopper.getCurrencies();
};

const getCurrencyByCode = async (code) => {
  return await globetopper.getCurrencyByCode(code);
};

module.exports = { getAllCurrencies, getCurrencyByCode };
