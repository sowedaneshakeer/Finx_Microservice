const express = require('express');
const router = express.Router();
const productController = require('../../controllers/giftcard/product.controller');
const authenticate = require('../../../middlewares/auth');

router.get('/', authenticate, productController.getAllProducts);
router.get('/:id', authenticate, productController.getProductById);

module.exports = router;
