const globetopper = require('../../providers/globetopper.provider');

const getAllCountries = async () => {
  return await globetopper.getCountries();
};

const getCountryByIso = async (iso) => {
  const countries = await globetopper.getCountries();
  return countries.find(c => c.iso2 === iso || c.iso3 === iso);
};

module.exports = { getAllCountries, getCountryByIso };
