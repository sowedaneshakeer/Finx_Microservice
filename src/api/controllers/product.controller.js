const productService = require('../../services/product.service');
const logger = require('../../utils/logger');

const getAllProducts = async (req, res) => {
  try {
    logger.info('GET /products called', { query: req.query });

    const result = await productService.getAllProducts({
      page: req.query.page,
      limit: req.query.limit,
      country: req.query.country,
      category: req.query.category,
      search: req.query.search,
      provider: req.query.provider
    });

    res.json({
      success: true,
      data: result.products,
      pagination: result.pagination,
      providers: result.providers
    });
  } catch (error) {
    logger.error('getAllProducts error:', { message: error.message });
    res.status(500).json({ success: false, error: 'SERVER_ERROR', message: error.message });
  }
};

module.exports = { getAllProducts };
