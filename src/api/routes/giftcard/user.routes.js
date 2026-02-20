const express = require('express');
const router = express.Router();
const userController = require('../../controllers/giftcard/user.controller');
const authenticate = require('../../../middlewares/auth');

router.get('/', authenticate, userController.getUserDetails);
router.get('/dtone-balance', authenticate, userController.getDtoneBalance);
router.get('/billers-balance', authenticate, userController.getBillersBalance);
router.get('/ppn-balance', authenticate, userController.getPpnBalance);

module.exports = router;
