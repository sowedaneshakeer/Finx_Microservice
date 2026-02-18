const express = require('express');
const router = express.Router();
const catalogueController = require('../../controllers/giftcard/catalogue.controller');
const authenticate = require('../../../middlewares/auth');

router.get('/', authenticate, catalogueController.getCatalogue);

module.exports = router;
