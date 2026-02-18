const axios = require('axios');
const FormData = require('form-data');
const logger = require('../utils/logger');
const { ENDPOINTS, ERROR_CODES } = require('../constants/globetopper.constants');

class GlobeTopperProvider {
  constructor() {
    this.client = axios.create({
      baseURL: process.env.GLOBETOPPER_BASE_URL,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.GLOBETOPPER_API_KEY}`
      },
      timeout: 20000
    });
  }

  async getCountries() {
    try {
      const response = await this.client.get(ENDPOINTS.COUNTRIES);
      return response.data.records;
    } catch (error) {
      logger.error('GlobeTopper API error - getCountries:', { message: error.message });
      throw error;
    }
  }

  async getProducts(filters = {}) {
    try {
      const response = await this.client.get(ENDPOINTS.PRODUCTS, {
        params: {
          countryCode: filters.countryCode,
          categoryID: filters.categoryId,
          typeID: filters.typeId
        }
      });
      return response.data.records || [];
    } catch (error) {
      logger.error('GlobeTopper API error - getProducts:', { message: error.message });
      throw error;
    }
  }

  async getCatalogue(filters = {}) {
    try {
      const response = await this.client.get(ENDPOINTS.CATALOGUE, {
        params: {
          productID: filters.productId,
          countryCode: filters.countryCode
        }
      });
      return response.data.records || [];
    } catch (error) {
      logger.error('GlobeTopper API error - getCatalogue:', { message: error.message });
      throw error;
    }
  }

  async getCurrencies() {
    try {
      const response = await this.client.get(ENDPOINTS.CURRENCIES);
      return response.data.records;
    } catch (error) {
      logger.error('GlobeTopper API error - getCurrencies:', { message: error.message });
      throw error;
    }
  }

  async getCurrencyByCode(code) {
    try {
      const response = await this.client.get(`${ENDPOINTS.CURRENCIES}/${code}`);
      return response.data.records[0];
    } catch (error) {
      logger.error('GlobeTopper API error - getCurrencyByCode:', { message: error.message });
      throw error;
    }
  }

  async createTransaction(productId, amount, userData) {
    try {
      const form = new FormData();
      form.append('first_name', userData.first_name);
      form.append('last_name', userData.last_name);
      form.append('email', userData.email);
      form.append('order_id', userData.order_id);
      if (userData.senderPhone) {
        form.append('senderPhone', userData.senderPhone);
      }

      const formClient = axios.create({
        baseURL: process.env.GLOBETOPPER_BASE_URL,
        headers: {
          ...form.getHeaders(),
          'Authorization': `Bearer ${process.env.GLOBETOPPER_API_KEY}`
        },
        timeout: 10000
      });

      const response = await formClient.post(
        `${ENDPOINTS.TRANSACTION_CREATE}/${productId}/${amount}`,
        form
      );

      const transaction = response.data.records[0];

      if (transaction.status === 0 && transaction.status_description === 'Success') {
        return {
          success: true,
          transactionId: transaction.trans_id,
          status: 'completed',
          details: transaction,
        };
      }

      return {
        success: false,
        transactionId: transaction.trans_id,
        status: 'unknown',
        rawResponse: transaction
      };
    } catch (error) {
      logger.error('Transaction failed:', {
        productId, amount,
        error: error.response?.data || error.message
      });

      if (error.response) {
        const errorData = error.response.data;
        const errorInfo = ERROR_CODES[errorData.responseCode] || {
          code: 'UNKNOWN_ERROR',
          message: errorData.responseMessage || 'Transaction failed'
        };
        const message = errorInfo.message || errorData.responseMessage;
        throw new Error(JSON.stringify({
          code: errorInfo.code,
          message: message,
          details: errorData
        }));
      }

      throw new Error(JSON.stringify({
        code: 'NETWORK_ERROR',
        message: 'Could not reach GlobeTopper API'
      }));
    }
  }

  async searchTransactions(filters = {}) {
    try {
      const response = await this.client.get(ENDPOINTS.TRANSACTION_SEARCH, {
        params: {
          startDate: filters.startDate,
          endDate: filters.endDate,
          status: filters.status,
          msisdn: filters.phoneNumber
        }
      });
      return response.data.records;
    } catch (error) {
      logger.error('GlobeTopper API error - searchTransactions:', { message: error.message });
      throw error;
    }
  }

  async getTransactionById(transactionId) {
    try {
      const response = await this.client.get(`${ENDPOINTS.TRANSACTION_SEARCH}/${transactionId}`);
      return response.data.records[0];
    } catch (error) {
      logger.error('GlobeTopper API error - getTransactionById:', { message: error.message });
      throw error;
    }
  }

  async getUserDetails() {
    try {
      const response = await this.client.get(ENDPOINTS.USER);
      return response.data.records[0];
    } catch (error) {
      logger.error('GlobeTopper API error - getUserDetails:', { message: error.message });
      throw error;
    }
  }
}

module.exports = new GlobeTopperProvider();
