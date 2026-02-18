const currencyService = require('../../../services/giftcard/currency.service');

const getAllCurrencies = async (req, res) => {
  try {
    const currencies = await currencyService.getAllCurrencies();
    res.json({ success: true, data: currencies });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

const getCurrencyByCode = async (req, res) => {
  try {
    const currency = await currencyService.getCurrencyByCode(req.params.code);
    if (!currency) {
      return res.status(404).json({ success: false, error: 'CURRENCY_NOT_FOUND' });
    }
    res.json({ success: true, data: currency });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

module.exports = { getAllCurrencies, getCurrencyByCode };
