const express = require('express');
const router = express.Router();
const currencyController = require('../../controllers/giftcard/currency.controller');
const authenticate = require('../../../middlewares/auth');

router.get('/', authenticate, currencyController.getAllCurrencies);
router.get('/:code', authenticate, currencyController.getCurrencyByCode);

module.exports = router;
