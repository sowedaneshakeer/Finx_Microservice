const axios = require('axios');
const logger = require('../utils/logger');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const MAX_RETRIES = 3;

class DtOneProvider {
  constructor() {
    this.baseURL = process.env.DTONE_BASE_URL;
  }

  async sendRequest(endpoint, method = 'GET', params = {}) {
    const url = `${this.baseURL.replace(/\/$/, '')}/${String(endpoint).replace(/^\//, '')}`;
    let attempts = 0;

    while (attempts < MAX_RETRIES) {
      try {
        const response = await axios({
          url,
          method,
          auth: {
            username: process.env.DTONE_USER,
            password: process.env.DTONE_PASS,
          },
          headers: { 'Content-Type': 'application/json' },
          params,
          timeout: 30000,
        });
        return response.data;
      } catch (err) {
        const status = err.response?.status;
        if (status === 429 || err.response?.headers?.['x-ratelimit-remaining'] === '0') {
          attempts++;
          const delay = 1000 * attempts;
          logger.warn(`DT-One rate limited, retry ${attempts}/${MAX_RETRIES} after ${delay}ms`);
          await sleep(delay);
          continue;
        }
        logger.error('DT-One Provider Error:', { url, status, message: err.message });
        throw err;
      }
    }
    throw new Error('DT-One request failed after ' + MAX_RETRIES + ' retries');
  }

  /**
   * Fetch a single page with full response (data + headers)
   */
  async sendRequestFull(endpoint, method = 'GET', params = {}) {
    const url = `${this.baseURL.replace(/\/$/, '')}/${String(endpoint).replace(/^\//, '')}`;
    let attempts = 0;

    while (attempts < MAX_RETRIES) {
      try {
        const response = await axios({
          url,
          method,
          auth: {
            username: process.env.DTONE_USER,
            password: process.env.DTONE_PASS,
          },
          headers: { 'Content-Type': 'application/json' },
          params,
          timeout: 30000,
        });
        return { data: response.data, headers: response.headers };
      } catch (err) {
        const status = err.response?.status;
        if (status === 429 || err.response?.headers?.['x-ratelimit-remaining'] === '0') {
          attempts++;
          const delay = 1500 * attempts;
          logger.warn(`DT-One rate limited, retry ${attempts}/${MAX_RETRIES} after ${delay}ms`);
          await sleep(delay);
          continue;
        }
        logger.error('DT-One Provider Error:', { url, status, message: err.message });
        throw err;
      }
    }
    throw new Error('DT-One request failed after ' + MAX_RETRIES + ' retries');
  }

  async getBalances() {
    return this.sendRequest('balances', 'GET');
  }

  async getProducts(filters = {}) {
    const params = {};
    if (filters.country) params.country_iso_code = filters.country;
    if (filters.type) params.type = filters.type;
    return this.sendRequest('products', 'GET', params);
  }

  /**
   * Fetch ALL products across all pages
   */
  async getAllProducts(filters = {}) {
    let allProducts = [];
    let page = 1;
    let totalPages = 1;
    const perPage = 50;
    let consecutiveErrors = 0;
    const MAX_PAGE_ERRORS = 3; // only abort after 3 consecutive page failures

    do {
      const params = { per_page: perPage, page };
      if (filters.country) params.country_iso_code = filters.country;

      try {
        const result = await this.sendRequestFull('products', 'GET', params);
        const products = Array.isArray(result.data) ? result.data : [];
        allProducts = allProducts.concat(products);
        consecutiveErrors = 0; // reset on success

        totalPages = parseInt(result.headers['x-total-pages'] || '1');
        const total = parseInt(result.headers['x-total'] || products.length);
        logger.info(`DT-One page ${page}/${totalPages} fetched (${products.length} items, total: ${total})`);
      } catch (err) {
        consecutiveErrors++;
        logger.error(`DT-One failed on page ${page} (attempt ${consecutiveErrors}/${MAX_PAGE_ERRORS})`, { error: err.message });
        if (consecutiveErrors >= MAX_PAGE_ERRORS) {
          logger.error(`DT-One aborting after ${MAX_PAGE_ERRORS} consecutive page failures at page ${page}`);
          break;
        }
        // wait longer before retrying same page
        await sleep(3000);
        continue; // retry same page
      }

      page++;
      if (page <= totalPages) await sleep(1200);
    } while (page <= totalPages);

    logger.info(`DT-One fetched ALL ${allProducts.length} products across ${page - 1} pages`);
    return allProducts;
  }

  async getProductById(id) {
    return this.sendRequest(`products/${id}`, 'GET');
  }

  async createTransaction(payload) {
    // Step 1: Create via async endpoint
    const url = `${this.baseURL.replace(/\/$/, '')}/async/transactions`;
    let attempts = 0;
    let asyncResult;

    while (attempts < MAX_RETRIES) {
      try {
        const response = await axios({
          url,
          method: 'POST',
          auth: {
            username: process.env.DTONE_USER,
            password: process.env.DTONE_PASS,
          },
          headers: { 'Content-Type': 'application/json' },
          data: payload,
          timeout: 30000,
        });
        asyncResult = response.data;
        break;
      } catch (err) {
        const status = err.response?.status;
        if (status === 429) {
          attempts++;
          const delay = 1000 * attempts;
          logger.warn(`DT-One rate limited on transaction, retry ${attempts}/${MAX_RETRIES}`);
          await sleep(delay);
          continue;
        }
        const errBody = err.response?.data;
        logger.error('DT-One createTransaction Error:', {
          status,
          message: err.message,
          errors: errBody?.errors || errBody,
        });
        // Attach DT-One error body to the error so it propagates
        const enrichedErr = new Error(err.message);
        enrichedErr.response = err.response;
        enrichedErr.dtoneErrors = errBody?.errors || errBody;
        throw enrichedErr;
      }
    }

    if (!asyncResult) {
      throw new Error('DT-One transaction failed after ' + MAX_RETRIES + ' retries');
    }

    // Step 2: Fetch full transaction details to get PIN/voucher data
    // Keep async status (most reliable) but merge supplemental data from GET
    const txId = asyncResult.id;
    if (txId) {
      await sleep(2000);
      try {
        const fullTx = await this.sendRequest(`transactions/${txId}`, 'GET');
        logger.info('DT-One full transaction fetched', { txId, hasPin: !!fullTx.pin, asyncStatus: asyncResult.status?.message, getStatus: fullTx.status?.message });
        // Merge: use async status + GET supplemental data (pin, benefits, prices)
        return {
          ...asyncResult,
          pin: fullTx.pin || asyncResult.pin || null,
          benefits: fullTx.benefits || asyncResult.benefits || [],
          prices: fullTx.prices || asyncResult.prices || null,
        };
      } catch (fetchErr) {
        logger.warn('DT-One: could not fetch full transaction, using async response', { txId, error: fetchErr.message });
      }
    }

    return asyncResult;
  }
}

module.exports = new DtOneProvider();
