const axios = require('axios');
const logger = require('../utils/logger');

class PpnProvider {
  constructor() {
    this.baseURL = process.env.PPN_BASE_URL;
  }

  async sendRequest(endpoint, method = 'GET', params = {}) {
    const url = `${this.baseURL}/${endpoint}`;
    try {
      const response = await axios({
        url,
        method,
        auth: {
          username: process.env.VALUETOPUP_USER,
          password: process.env.VALUETOPUP_PASS,
        },
        headers: { 'Content-Type': 'application/json' },
        params,
        timeout: 20000,
      });
      return response.data;
    } catch (error) {
      logger.error('PPN Provider Error:', { url, message: error.message, status: error.response?.status });
      throw error;
    }
  }

  async getProducts(filters = {}) {
    const params = {};
    if (filters.country) params.country = filters.country;
    return this.sendRequest('catalog/skus', 'GET', params);
  }

  async getProductById(skuCode) {
    return this.sendRequest('catalog/skus', 'GET', { skuCode });
  }

  async createTransaction(endpoint, payload) {
    const url = `${this.baseURL}/${endpoint}`;
    try {
      const response = await axios({
        url,
        method: 'POST',
        auth: {
          username: process.env.VALUETOPUP_USER,
          password: process.env.VALUETOPUP_PASS,
        },
        headers: { 'Content-Type': 'application/json' },
        data: payload,
        timeout: 30000,
      });
      return response.data;
    } catch (error) {
      logger.error('PPN createTransaction Error:', { url, message: error.message, status: error.response?.status });
      throw error;
    }
  }
}

module.exports = new PpnProvider();
