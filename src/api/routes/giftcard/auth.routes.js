const express = require('express');
const router = express.Router();
const authController = require('../../controllers/giftcard/auth.controller');

router.post('/login', authController.login);

module.exports = router;
