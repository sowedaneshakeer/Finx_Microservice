const globetopper = require('../../providers/globetopper.provider');
const dtoneProvider = require('../../providers/dtone.provider');
const billersProvider = require('../../providers/billers.provider');
const ppnProvider = require('../../providers/ppn.provider');
const logger = require('../../utils/logger');
const { decodeProductId } = require('../../utils/providerCodes');

/**
 * Create a transaction routed to the correct provider
 */
const createTransaction = async ({ provider, ...params }) => {
  const providerName = (provider || 'globetopper').toLowerCase();
  logger.info(`Creating transaction via provider: ${providerName}`, { productId: params.productId });

  switch (providerName) {
    case 'globetopper':
      return createGlobetopperTransaction(params);
    case 'dtone':
      return createDtoneTransaction(params);
    case 'billers':
      return createBillersTransaction(params);
    case 'ppn':
      return createPpnTransaction(params);
    default:
      throw new Error(`Unknown provider: ${providerName}`);
  }
};

// ─── GlobeTopper ────────────────────────────────────────────────────────────
async function createGlobetopperTransaction({ productId, amount, email, firstName, lastName, phoneNumber }) {
  const { rawId } = decodeProductId(productId);
  return await globetopper.createTransaction(rawId, amount, {
    first_name: firstName,
    last_name: lastName,
    senderPhone: phoneNumber,
    email,
    order_id: `ORDER_${Date.now()}_${Math.floor(Math.random() * 1000)}`
  });
}

// ─── DT-One ─────────────────────────────────────────────────────────────────
async function createDtoneTransaction(params) {
  const {
    productId, // dtone_XXXX or raw ID
    calculationMode,
    sourceAmount, sourceCurrency,
    destinationAmount, destinationCurrency,
    creditPartyType, creditPartyValue,
    senderFields, beneficiaryFields,
    debitPartyFields, statementFields, additionalFields,
    autoConfirm, callbackUrl
  } = params;

  const { rawId: realProductId } = decodeProductId(productId);
  const externalId = `EXT-${Date.now()}-${Math.floor(Math.random() * 1000)}`;

  const payload = {
    product_id: parseInt(realProductId),
    external_id: externalId,
    auto_confirm: autoConfirm === 'true' || autoConfirm === true,
  };

  // Credit party identifier
  if (creditPartyValue) {
    const cpType = creditPartyType || 'mobile_number';
    payload.credit_party_identifier = { [cpType]: creditPartyValue };
  }

  // Calculation mode for ranged products
  if (calculationMode) {
    payload.calculation_mode = calculationMode;
    if (calculationMode === 'SOURCE_AMOUNT' && sourceAmount) {
      payload.source = { amount: parseFloat(sourceAmount), unit: sourceCurrency, unit_type: 'CURRENCY' };
    } else if (calculationMode === 'DESTINATION_AMOUNT' && destinationAmount) {
      payload.destination = { amount: parseFloat(destinationAmount), unit: destinationCurrency, unit_type: 'CURRENCY' };
    }
  }

  // Dynamic field groups
  if (senderFields && Object.keys(senderFields).length > 0) payload.sender = senderFields;
  if (beneficiaryFields && Object.keys(beneficiaryFields).length > 0) payload.beneficiary = beneficiaryFields;
  if (debitPartyFields && Object.keys(debitPartyFields).length > 0) payload.debit_party_identifier = debitPartyFields;
  if (statementFields && Object.keys(statementFields).length > 0) payload.statement_identifier = statementFields;
  if (additionalFields && Object.keys(additionalFields).length > 0) payload.additional_identifier = additionalFields;

  if (callbackUrl) payload.callback_url = callbackUrl;

  logger.info('DT-One transaction payload', JSON.stringify(payload));

  const result = await dtoneProvider.createTransaction(payload);

  // Log raw response keys and PIN data for debugging
  logger.info('DT-One raw response keys', { keys: Object.keys(result), hasPin: !!result.pin, pinKeys: result.pin ? Object.keys(result.pin) : null });
  if (result.pin) {
    logger.info('DT-One PIN data', { code: result.pin.code ? '***present***' : null, serial: result.pin.serial || null, validity: result.pin.validity || null });
  }

  // Normalize response
  const status = result.status?.message || result.status?.class?.message || 'UNKNOWN';

  // Extract PIN/voucher data (returned for PIN_PURCHASE products: eSIM, Gift Cards, PIN vouchers)
  const pinData = result.pin || null;
  const benefitsData = result.benefits || [];
  const productData = result.product || {};

  return {
    success: status === 'CONFIRMED' || status === 'COMPLETED',
    transactionId: result.id || externalId,
    externalId,
    status: status.toLowerCase(),
    details: result,
    rawResponse: result,
    // Fields needed by orchestrator for display
    final_amount: result.prices?.retail?.amount || result.destination?.amount || parseFloat(sourceAmount) || result.source?.amount || 0,
    summary: {
      TotalCustomerCostUSD: result.prices?.retail?.amount || result.destination?.amount || 0,
    },
    operator: {
      name: result.product?.name || result.operator?.name || '',
      id: realProductId
    },
    // PIN/voucher delivery data (for PIN_PURCHASE products)
    pin: pinData,
    benefits: benefitsData,
    product: productData,
    prices: result.prices || null,
    // Delivery metadata (passed through from orchestrator for future email use)
    deliveryEmail: params.deliveryEmail || '',
  };
}

// ─── Billers ────────────────────────────────────────────────────────────────
async function createBillersTransaction(params) {
  const {
    productId, // billers_XXXX
    sku,
    inputs,
    amount,
    customerId,
    doInquiry,
    billerName,
    billerCountry,
    receivingCurrency,
    billerType,
  } = params;

  const { rawId: billerId } = decodeProductId(productId);
  const transactionId = `TXN-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const entityCustomerId = customerId || `CUST-${Date.now()}`;

  let finalInputs = inputs;

  const txMeta = {
    billerName: billerName || '',
    billerCountry: billerCountry || '',
    receivingCurrency: receivingCurrency || '',
    billerId,
  };

  // Step 1: Inquiry if requested (optional - some SKUs don't support it)
  let inquiryFailed = false;
  let inquiryError = null;
  if (doInquiry === 'true' || doInquiry === true) {
    logger.info('Billers: performing inquiry first', { billerId, sku });
    try {
      const inquiryResult = await billersProvider.inquiryPayment({
        billerId, sku,
        inputs: finalInputs,
        customerId: entityCustomerId,
        transactionId,
      });

      if (inquiryResult.ResponseCode === '00') {
        // Use Output1 from inquiry as inputs for payment
        finalInputs = inquiryResult.Output1 || finalInputs;
        logger.info('Billers: inquiry successful', { billAmountDue: inquiryResult.BillAmountDue });
      } else if (inquiryResult.ResponseCode === '11') {
        // Inquiry not available for this SKU - skip and proceed to payment
        logger.info('Billers: inquiry not available for SKU, proceeding to payment directly', { billerId, sku });
      } else {
        // Inquiry returned a non-success code (e.g. 07 = Invalid reference)
        // Don't throw — return a failed transaction response so the user sees it on the result page
        inquiryFailed = true;
        inquiryError = inquiryResult;
        logger.warn('Billers: inquiry failed', {
          responseCode: inquiryResult.ResponseCode,
          responseMessage: inquiryResult.ResponseMessage,
          billerId, sku
        });
      }
    } catch (inquiryErr) {
      // If inquiry fails with a network/API error, check if it's the "not available" response
      const errData = inquiryErr.response?.data;
      if (errData?.ResponseCode === '11' || errData?.responseCode === '11') {
        logger.info('Billers: inquiry not available for SKU (API error), proceeding to payment', { billerId, sku });
      } else {
        logger.warn('Billers: inquiry failed, proceeding to payment anyway', { error: inquiryErr.message });
      }
    }
  }

  // If inquiry explicitly failed (invalid reference, etc.), return failure without calling processPayment
  if (inquiryFailed && inquiryError) {
    return normalizeBillersResponse(
      { ...inquiryError, ResponseCode: inquiryError.ResponseCode },
      transactionId, amount, txMeta
    );
  }

  // Step 2: Process payment
  logger.info('Billers: processing payment', { billerId, sku, amount });
  const result = await billersProvider.processPayment({
    billerId, sku,
    inputs: finalInputs,
    amount: parseFloat(amount),
    customerId: entityCustomerId,
    transactionId,
  });

  // Handle "in progress" status - poll for final result
  if (result.ResponseCode === '10') {
    logger.info('Billers: payment in progress, polling status', { transactionId });
    let retries = 0;
    const maxRetries = 5;
    while (retries < maxRetries) {
      await new Promise(r => setTimeout(r, 3000));
      const statusResult = await billersProvider.verifyPaymentStatus(transactionId);
      if (statusResult.ResponseCode !== '10') {
        return normalizeBillersResponse(statusResult, transactionId, amount, txMeta);
      }
      retries++;
    }
    // Still in progress after polling
    return normalizeBillersResponse(result, transactionId, amount, txMeta);
  }

  return normalizeBillersResponse(result, transactionId, amount, txMeta);
}

function normalizeBillersResponse(result, transactionId, amount, meta = {}) {
  const isSuccess = result.ResponseCode === '00';
  const providerFxRate = parseFloat(result.FXRate) || 0;  // AED→local rate from processPayment
  const receivingAmount = parseFloat(amount) || 0;

  // Calculate AED amount from processPayment FXRate (AED→local)
  // USD conversion not available yet — will be added when AED→USD rate source is confirmed
  const baseCurrency = result.BaseCurrency || 'AED';
  const baseCurrencyAmount = providerFxRate > 0 ? receivingAmount / providerFxRate : 0;

  const billersMetaBlock = {
    billerName: meta.billerName || '',
    billerCountry: meta.billerCountry || '',
    receivingAmount,
    receivingCurrency: meta.receivingCurrency || '',
    baseCurrencyAmount: Math.round(baseCurrencyAmount * 100) / 100,
    baseCurrency,
    providerFxRate,
    fxDateTime: result.ResponseDateTime || '',
  };

  return {
    success: isSuccess,
    transactionId: result.TransactionID || transactionId,
    externalId: transactionId,
    status: isSuccess ? 'completed' : (result.ResponseCode === '10' ? 'pending' : 'failed'),
    details: {
      ...result,
      billers_meta: billersMetaBlock,
    },
    rawResponse: { ...result, billers_meta: billersMetaBlock },
    final_amount: 0,  // No USD conversion yet
    billers_meta: billersMetaBlock,
    summary: {
      TotalCustomerCostUSD: 0,  // No USD conversion yet
    },
    operator: {
      name: meta.billerName || result.BillerName || '',
      id: meta.billerId || result.BillerID || ''
    },
    confirmationNumber: result.ConfirmationNumber,
    statusMessage: result.ResponseMessage,
  };
}

// ─── PPN (Prepay Nation / Valuetopup) ───────────────────────────────────────
async function createPpnTransaction(params) {
  const {
    productId, // ppn_XXXX
    ppnCategory,
    amount, mobile, accountNumber, recipient,
    simNumber, zipCode, email,
    firstName, lastName,
    transactionCurrencyCode,
  } = params;

  const { rawId: ppnRawId } = decodeProductId(productId);
  const skuId = parseInt(ppnRawId);
  const correlationId = `TXN-${(ppnCategory || 'GC').toUpperCase()}-${skuId}-${Date.now()}`;

  // Route to the correct PPN endpoint based on category
  // API docs: /api/v2/transaction/topup, /transaction/pin, /transaction/billpay,
  //           /transaction/giftcard/order, /transaction/sim/activate, /esim/order
  const category = (ppnCategory || 'giftcard').toLowerCase();
  let endpoint = '';
  let payload = { skuId, correlationId };

  switch (category) {
    case 'pin':
      endpoint = 'transaction/pin';
      if (recipient || mobile) payload.recipient = recipient || mobile;
      break;
    case 'rtr':
      endpoint = 'transaction/topup';
      payload.amount = parseFloat(amount);
      payload.mobile = mobile;
      if (transactionCurrencyCode) payload.transactionCurrencyCode = transactionCurrencyCode;
      break;
    case 'billpay':
      endpoint = 'transaction/billpay';
      payload.amount = parseFloat(amount);
      payload.accountNumber = accountNumber;
      if (transactionCurrencyCode) payload.transactionCurrencyCode = transactionCurrencyCode;
      break;
    case 'sim':
      endpoint = 'transaction/sim/activate';
      payload.simNumber = simNumber;
      payload.zipCode = parseInt(zipCode) || 0;
      if (email) payload.email = email;
      break;
    case 'esim':
      endpoint = 'esim/order';
      break;
    default: // gift card
      endpoint = 'transaction/giftcard/order';
      if (amount) payload.amount = parseFloat(amount);
      if (firstName) payload.firstName = firstName;
      if (lastName) payload.lastName = lastName;
      if (recipient || mobile) payload.recipient = recipient || mobile;
      if (transactionCurrencyCode) payload.transactionCurrencyCode = transactionCurrencyCode;
      break;
  }

  logger.info('PPN transaction', { endpoint, correlationId, category, payload: JSON.stringify(payload) });
  const result = await ppnProvider.createTransaction(endpoint, payload);

  // PPN response format: { responseCode: "000", responseMessage: null, payLoad: {...} }
  // responseCode "000" = success, anything else = failure
  const responseCode = result?.responseCode;
  const isSuccess = responseCode === '000';
  const pay = result?.payLoad || {};

  logger.info('PPN transaction result', {
    responseCode,
    isSuccess,
    transactionId: pay.transactionId,
    invoiceAmount: pay.invoiceAmount,
    hasPins: !!(pay.pins && pay.pins.length),
    hasGiftCard: !!pay.giftCardDetail,
    hasEsim: !!pay.esimDetail,
  });

  // For gift cards, fetch card details (cardNumber, pin, expirationDate, certificateLink)
  let giftCardInfo = null;
  if (isSuccess && category === 'giftcard' && pay.transactionId) {
    try {
      const gcResult = await ppnProvider.fetchGiftCardInfo(pay.transactionId);
      if (gcResult?.responseCode === '000' && gcResult?.payLoad) {
        giftCardInfo = gcResult.payLoad;
        logger.info('PPN gift card info fetched', { transactionId: pay.transactionId, hasCardNumber: !!giftCardInfo.cardNumber });
      }
    } catch (gcErr) {
      logger.warn('PPN: could not fetch gift card info', { transactionId: pay.transactionId, error: gcErr.message });
    }
  }

  // If failed, build error info
  if (!isSuccess) {
    const errMsg = result?.responseMessage || 'Transaction failed';
    logger.warn('PPN transaction failed', { responseCode, responseMessage: errMsg, correlationId });
    const err = new Error(JSON.stringify({ code: `PPN_${responseCode}`, message: errMsg }));
    err.ppnResponseCode = responseCode;
    err.ppnResponseMessage = errMsg;
    throw err;
  }

  return {
    success: true,
    transactionId: pay.transactionId || correlationId,
    externalId: correlationId,
    status: 'completed',
    details: pay,
    rawResponse: result,
    final_amount: pay.invoiceAmount || parseFloat(amount) || 0,
    summary: {
      TotalCustomerCostUSD: pay.invoiceAmount || parseFloat(amount) || 0,
    },
    operator: {
      name: pay.product?.productName || pay.product?.operatorName || '',
      id: String(skuId),
    },
    // PPN-specific data for display
    ppnPayload: pay,
    pins: pay.pins || [],
    giftCardDetail: giftCardInfo || pay.giftCardDetail || null,
    esimDetail: pay.esimDetail || null,
    topupDetail: pay.topupDetail || null,
    simInfo: pay.simInfo || null,
    billPaymentDetail: pay.billPaymentDetail || null,
    invoiceAmount: pay.invoiceAmount || 0,
    faceValue: pay.faceValue || 0,
    discount: pay.discount || 0,
    fee: pay.fee || 0,
    transactionDate: pay.transactionDate || '',
  };
}

const searchTransactions = async (filters) => {
  return await globetopper.searchTransactions(filters);
};

const getTransactionById = async (id) => {
  return await globetopper.getTransactionById(id);
};

module.exports = { createTransaction, searchTransactions, getTransactionById };
