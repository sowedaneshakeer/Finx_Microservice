const express = require('express');
const router = express.Router();
const transactionController = require('../../controllers/giftcard/transaction.controller');
const authenticate = require('../../../middlewares/auth');

router.post('/', authenticate, transactionController.createTransaction);
router.get('/', authenticate, transactionController.searchTransactions);
router.get('/:id', authenticate, transactionController.getTransactionById);

module.exports = router;
