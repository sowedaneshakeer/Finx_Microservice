const axios = require('axios');
const logger = require('../utils/logger');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const MAX_RETRIES = 3;

class PpnProvider {
  constructor() {
    this.baseURL = process.env.PPN_BASE_URL;
  }

  _buildUrl(endpoint) {
    return `${this.baseURL.replace(/\/$/, '')}/${String(endpoint).replace(/^\//, '')}`;
  }

  _authHeaders() {
    return {
      auth: {
        username: process.env.VALUETOPUP_USER,
        password: process.env.VALUETOPUP_PASS,
      },
      headers: { 'Content-Type': 'application/json' },
    };
  }

  /**
   * Generic GET request for catalog/lookup endpoints
   */
  async sendRequest(endpoint, method = 'GET', params = {}) {
    const url = this._buildUrl(endpoint);
    try {
      const response = await axios({
        url,
        method,
        ...this._authHeaders(),
        params,
        timeout: 20000,
      });
      return response.data;
    } catch (error) {
      logger.error('PPN Provider Error:', { url, message: error.message, status: error.response?.status });
      throw error;
    }
  }

  /**
   * POST transaction with retry on rate limit / timeout
   * PPN response format: { responseCode: "000", responseMessage: null, payLoad: {...} }
   * responseCode "000" = success, anything else = failure (even with HTTP 200)
   */
  async createTransaction(endpoint, payload) {
    const url = this._buildUrl(endpoint);
    let attempts = 0;

    while (attempts < MAX_RETRIES) {
      try {
        const response = await axios({
          url,
          method: 'POST',
          ...this._authHeaders(),
          data: payload,
          timeout: 60000, // PPN can take up to 2 mins; use 60s client-side
        });

        const result = response.data;
        logger.info('PPN raw response', { endpoint, responseCode: result?.responseCode, hasPayload: !!result?.payLoad });

        return result;
      } catch (error) {
        const status = error.response?.status;
        // Retry on 429 or 5xx
        if (status === 429 || (status >= 500 && status < 600)) {
          attempts++;
          const delay = 2000 * attempts;
          logger.warn(`PPN rate limited / server error, retry ${attempts}/${MAX_RETRIES} after ${delay}ms`, { status });
          await sleep(delay);
          continue;
        }
        // If PPN returned HTTP 200 with error body (unlikely on axios throw, but handle)
        const errBody = error.response?.data;
        logger.error('PPN createTransaction Error:', {
          url, status,
          message: error.message,
          responseCode: errBody?.responseCode,
          responseMessage: errBody?.responseMessage,
        });
        const enrichedErr = new Error(error.message);
        enrichedErr.response = error.response;
        enrichedErr.ppnResponseCode = errBody?.responseCode;
        enrichedErr.ppnResponseMessage = errBody?.responseMessage;
        throw enrichedErr;
      }
    }
    throw new Error('PPN transaction failed after ' + MAX_RETRIES + ' retries');
  }

  async getBalance() {
    return this.sendRequest('account/balance', 'GET');
  }

  // ─── Catalog endpoints ──────────────────────────────────────────────────

  async getProducts(filters = {}) {
    const params = {};
    if (filters.country) params.countryCode = filters.country;
    if (filters.operatorId) params.operatorId = filters.operatorId;
    if (filters.categoryId) params.categoryId = filters.categoryId;
    return this.sendRequest('catalog/skus', 'GET', params);
  }

  async getGiftCardProducts(filters = {}) {
    const params = {};
    if (filters.country) params.countryCode = filters.country;
    if (filters.productId) params.productId = filters.productId;
    if (filters.cursor) params.cursor = filters.cursor;
    if (filters.pageSize) params.pageSize = filters.pageSize;
    return this.sendRequest('catalog/skus/giftcards', 'GET', params);
  }

  async getProductById(skuId) {
    return this.sendRequest('catalog/skus', 'GET', { skuId: parseInt(skuId) });
  }

  async getOperators(filters = {}) {
    const params = {};
    if (filters.operatorId) params.operatorId = filters.operatorId;
    if (filters.countryCode) params.countryCode = filters.countryCode;
    return this.sendRequest('catalog/operators', 'GET', params);
  }

  async getExchangeRate(skuId) {
    return this.sendRequest(`catalog/sku/exchangeRate/${skuId}`, 'GET');
  }

  async lookupMobile(mobile) {
    return this.sendRequest(`lookup/mobile/${mobile}`, 'GET');
  }

  // ─── Transaction status ─────────────────────────────────────────────────

  async checkTransactionStatus(correlationId) {
    return this.sendRequest(`transaction/status/${correlationId}`, 'GET');
  }

  async fetchGiftCardInfo(transactionId) {
    return this.sendRequest(`transaction/giftcard/fetch/${transactionId}`, 'GET');
  }
}

module.exports = new PpnProvider();
