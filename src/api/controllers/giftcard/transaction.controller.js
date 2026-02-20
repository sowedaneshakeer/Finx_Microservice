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

    // Always return normalized data (same structure for success and failure)
    return res.json({
      success: true,
      ...(result.success ? {} : { warning: 'UNUSUAL_TRANSACTION_STATUS' }),
      data: {
        ...result.details,
        // Normalized fields override raw provider response
        transactionId: result.transactionId,
        externalId: result.externalId,
        status: result.status,
        success: result.success,
        provider: providerName,
        final_amount: result.final_amount,
        summary: result.summary,
        operator: result.operator,
        // PIN/voucher data (DT-One PIN_PURCHASE products)
        pin: result.pin || null,
        benefits: result.benefits || null,
        prices: result.prices || null,
        deliveryEmail: result.deliveryEmail || '',
        // PPN-specific data
        pins: result.pins || null,
        giftCardDetail: result.giftCardDetail || null,
        esimDetail: result.esimDetail || null,
        topupDetail: result.topupDetail || null,
        simInfo: result.simInfo || null,
        billPaymentDetail: result.billPaymentDetail || null,
        invoiceAmount: result.invoiceAmount || null,
        faceValue: result.faceValue || null,
        discount: result.discount || null,
        fee: result.fee || null,
        transactionDate: result.transactionDate || null,
      }
    });

  } catch (error) {
    logger.error('Transaction error:', { message: error.message, provider: req.body?.provider, dtoneErrors: error.dtoneErrors || null, ppnCode: error.ppnResponseCode || null });

    let errorDetails;
    try {
      errorDetails = JSON.parse(error.message);
    } catch {
      errorDetails = { code: 'UNKNOWN_ERROR', message: error.message };
    }

    // PPN-specific error details
    if (error.ppnResponseCode) {
      errorDetails.code = `PPN_${error.ppnResponseCode}`;
      errorDetails.message = error.ppnResponseMessage || errorDetails.message;
    }

    // Include provider-specific error details (DT-One errors, etc.)
    const providerErrors = error.dtoneErrors || error.response?.data?.errors || error.response?.data || null;
    if (providerErrors) {
      errorDetails.providerErrors = providerErrors;
      // Extract first meaningful error message from DT-One
      if (Array.isArray(providerErrors) && providerErrors.length > 0) {
        errorDetails.message = providerErrors[0].message || providerErrors[0].code || errorDetails.message;
        errorDetails.code = providerErrors[0].code || errorDetails.code;
      }
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
      providerErrors: providerErrors || undefined,
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
