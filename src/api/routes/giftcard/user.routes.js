const express = require('express');
const router = express.Router();
const userController = require('../../controllers/giftcard/user.controller');
const authenticate = require('../../../middlewares/auth');

router.get('/', authenticate, userController.getUserDetails);

module.exports = router;
