const express = require('express');
const router = express.Router();
const productController = require('../controllers/product.controller');
const authenticate = require('../../middlewares/auth');

// GET /api/v1/products - unified products from all providers
router.get('/', authenticate, productController.getAllProducts);

module.exports = router;
