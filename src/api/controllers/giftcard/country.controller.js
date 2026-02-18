const countryService = require('../../../services/giftcard/country.service');

const getAllCountries = async (req, res) => {
  try {
    const countries = await countryService.getAllCountries();
    res.json({ success: true, data: countries });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

const getCountryByIso = async (req, res) => {
  try {
    const country = await countryService.getCountryByIso(req.params.iso);
    if (!country) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND' });
    }
    res.json({ success: true, data: country });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

module.exports = { getAllCountries, getCountryByIso };
