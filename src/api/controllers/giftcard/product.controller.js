const productService = require('../../../services/product.service');
const logger = require('../../../utils/logger');

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

// const getAllProducts = async (req, res) => {
//   try {
//     const products = await productService.getAllProducts({
//       countryCode: req.query.country,
//       categoryId: req.query.category,
//       typeId: req.query.type
//     });
//     res.json({ success: true, data: products });
//   } catch (error) {
//     res.status(500).json({ success: false, error: 'SERVER_ERROR' });
//   }
// };

const getProductById = async (req, res) => {
  try {
    const product = await productService.getProductById(req.params.id);
    if (!product) {
      return res.status(404).json({
        success: false,
        error: 'PRODUCT_NOT_FOUND',
        message: `No product found with ID ${req.params.id}`
      });
    }
    res.json({ success: true, data: product });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'SERVER_ERROR',
      details: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
};

module.exports = { getAllProducts, getProductById };
