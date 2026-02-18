const transactionService = require('../../../services/giftcard/transaction.service');
const logger = require('../../../utils/logger');

const createTransaction = async (req, res) => {
  try {
    const { provider } = req.body;
    const providerName = (provider || 'globetopper').toLowerCase();

    // Validate minimum required fields based on provider
    if (providerName === 'globetopper') {
      const { productId, amount, email, firstName, lastName } = req.body;
      const requiredFields = { productId, amount, email, firstName, lastName };
      const missingFields = Object.entries(requiredFields)
        .filter(([_, value]) => !value)
        .map(([key]) => key);
      if (missingFields.length > 0) {
        return res.status(400).json({ success: false, error: 'MISSING_REQUIRED_FIELDS', missingFields });
      }
    } else if (providerName === 'dtone') {
      if (!req.body.productId) {
        return res.status(400).json({ success: false, error: 'MISSING_REQUIRED_FIELDS', missingFields: ['productId'] });
      }
    } else if (providerName === 'billers') {
      const { productId, sku, amount } = req.body;
      const missingFields = [];
      if (!productId) missingFields.push('productId');
      if (!sku) missingFields.push('sku');
      if (!amount) missingFields.push('amount');
      if (missingFields.length > 0) {
        return res.status(400).json({ success: false, error: 'MISSING_REQUIRED_FIELDS', missingFields });
      }
    } else if (providerName === 'ppn') {
      if (!req.body.productId) {
        return res.status(400).json({ success: false, error: 'MISSING_REQUIRED_FIELDS', missingFields: ['productId'] });
      }
    }

    // Pass all body params to the transaction service
    const result = await transactionService.createTransaction(req.body);

    if (result.success) {
      return res.json({
        success: true,
        data: {
          transactionId: result.transactionId,
          externalId: result.externalId,
          status: result.status,
          provider: providerName,
          ...result.details
        }
      });
    }

    return res.json({
      success: true,
      warning: 'UNUSUAL_TRANSACTION_STATUS',
      data: result.rawResponse
    });

  } catch (error) {
    logger.error('Transaction error:', { message: error.message, provider: req.body?.provider });

    let errorDetails;
    try {
      errorDetails = JSON.parse(error.message);
    } catch {
      errorDetails = { code: 'UNKNOWN_ERROR', message: error.message };
    }

    if (errorDetails.code === 'OUT_OF_STOCK') {
      return res.status(409).json({
        success: false, error: errorDetails.code, message: errorDetails.message,
        suggestedActions: ['Try different amount', 'Check similar products']
      });
    }

    const statusCode = error.response?.status || 500;
    return res.status(statusCode).json({
      success: false,
      error: errorDetails.code || 'TRANSACTION_FAILED',
      message: errorDetails.message,
      ...(process.env.NODE_ENV === 'development' && { stack: error.stack })
    });
  }
};

const searchTransactions = async (req, res) => {
  try {
    const transactions = await transactionService.searchTransactions({
      startDate: req.query.startDate,
      endDate: req.query.endDate,
      status: req.query.status,
      phoneNumber: req.query.phone
    });
    res.json({ success: true, data: transactions });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

const getTransactionById = async (req, res) => {
  try {
    const transaction = await transactionService.getTransactionById(req.params.id);
    if (!transaction) {
      return res.status(404).json({ success: false, error: 'TRANSACTION_NOT_FOUND' });
    }
    res.json({ success: true, data: transaction });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

module.exports = { createTransaction, searchTransactions, getTransactionById };
