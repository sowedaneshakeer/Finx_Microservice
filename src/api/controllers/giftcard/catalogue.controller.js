const catalogueService = require('../../../services/giftcard/catalogue.service');

const getCatalogue = async (req, res) => {
  try {
    const catalogue = await catalogueService.getCatalogue({
      productId: req.query.productId,
      countryCode: req.query.countryCode
    });
    res.json({ success: true, data: catalogue });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

module.exports = { getCatalogue };
