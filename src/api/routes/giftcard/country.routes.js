const express = require('express');
const router = express.Router();
const countryController = require('../../controllers/giftcard/country.controller');
const authenticate = require('../../../middlewares/auth');

router.get('/', authenticate, countryController.getAllCountries);
router.get('/:iso', authenticate, countryController.getCountryByIso);

module.exports = router;
