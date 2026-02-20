const axios = require('axios');
const logger = require('../utils/logger');

class BillersProvider {
  constructor() {
    this.client = axios.create({
      baseURL: process.env.BILRS_API_URL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.BILRS_API_TOKEN}`
      }
    });
    // In-memory cache for iocatalog (SKU → { data, timestamp })
    this._ioCatalogCache = new Map();
    this._ioCatalogCacheTTL = 30 * 60 * 1000; // 30 minutes
  }

  async getBalance(currency = 'AED') {
    try {
      const response = await this.client.post('/balance', { Currency: currency });
      return response.data;
    } catch (error) {
      logger.error('Billers getBalance Error:', { message: error.message, status: error.response?.status });
      throw error;
    }
  }

  async getBillers(params = {}) {
    try {
      const response = await this.client.post('/billercatalog', params);
      return response.data;
    } catch (error) {
      logger.error('Billers Provider Error:', { message: error.message, status: error.response?.status });
      throw error;
    }
  }

  /**
   * Fetch ALL billers across all pages
   */
  async getAllBillers(filters = {}) {
    let allBillers = [];
    let page = 1;
    let totalPages = 1;
    const pageSize = 200;

    do {
      try {
        const params = { ...filters, Page: page, PageSize: pageSize };
        const response = await this.client.post('/billercatalog', params);
        const data = response.data;

        const billers = data?.Data || [];
        allBillers = allBillers.concat(billers);
        totalPages = data?.TotalPages || 1;

        logger.info(`Billers page ${page}/${totalPages} fetched (${billers.length} items, total: ${data?.TotalRecords || 0})`);
      } catch (error) {
        logger.error(`Billers failed on page ${page}`, { error: error.message });
        break;
      }

      page++;
    } while (page <= totalPages);

    logger.info(`Billers fetched ALL ${allBillers.length} billers across ${page - 1} pages`);
    return allBillers;
  }

  async getSkus(billerId) {
    try {
      const response = await this.client.post('/skucatalog', { BillerID: billerId });
      return response.data;
    } catch (error) {
      logger.error('Billers getSkus Error:', { message: error.message, billerId });
      throw error;
    }
  }

  async getSkuInputs(sku) {
    try {
      // Check cache first — iocatalog data doesn't change frequently
      const cached = this._ioCatalogCache.get(sku);
      if (cached && (Date.now() - cached.timestamp) < this._ioCatalogCacheTTL) {
        logger.info(`iocatalog cache HIT for SKU: ${sku} (${cached.data.length} records)`);
        return { Data: cached.data };
      }

      // IO catalog returns entries for ALL billers sharing this SKU (can be 2000+ records).
      // API caps PageSize at 1000, so we must paginate through all pages.
      let allData = [];
      let page = 1;
      let totalPages = 1;
      const pageSize = 1000;

      do {
        const response = await this.client.post('/iocatalog', { SKU: sku, Page: page, PageSize: pageSize });
        const data = response.data;
        const records = data?.Data || [];
        allData = allData.concat(records);
        totalPages = data?.TotalPages || 1;

        logger.info(`iocatalog page ${page}/${totalPages} fetched (${records.length} items, total: ${data?.TotalRecords || 0}, SKU: ${sku})`);
        page++;
      } while (page <= totalPages);

      logger.info(`iocatalog fetched ALL ${allData.length} records across ${page - 1} pages for SKU: ${sku}`);

      // Cache the result
      this._ioCatalogCache.set(sku, { data: allData, timestamp: Date.now() });

      return { Data: allData };
    } catch (error) {
      logger.error('Billers getSkuInputs Error:', { message: error.message, sku });
      throw error;
    }
  }

  async inquiryPayment({ billerId, sku, inputs, customerId, transactionId }) {
    try {
      const response = await this.client.post('/amountdue', {
        BillerID: billerId,
        SKU: sku,
        Inputs: inputs,
        EntityCustomerID: customerId,
        EntityTransactionID: transactionId
      });
      return response.data;
    } catch (error) {
      logger.error('Billers inquiryPayment Error:', { message: error.message, billerId, sku });
      throw error;
    }
  }

  async processPayment({ billerId, sku, inputs, amount, customerId, transactionId, billIds }) {
    try {
      const payload = {
        BillerID: billerId,
        SKU: sku,
        Inputs: inputs,
        Amount: amount,
        EntityCustomerID: customerId,
        EntityTransactionID: transactionId
      };
      if (billIds && billIds.length > 0) payload.BillIds = billIds;
      const response = await this.client.post('/processpayment', payload);
      return response.data;
    } catch (error) {
      logger.error('Billers processPayment Error:', { message: error.message, billerId, sku });
      throw error;
    }
  }

  async getDailyFXRate(baseCurrency, settlementCurrency) {
    try {
      const response = await this.client.post('/dailyfxrate', {
        BaseCurrency: baseCurrency,
        SettlementCurrency: settlementCurrency,
        Page: 1,
        PageSize: 100
      });
      return response.data;
    } catch (error) {
      logger.error('Billers getDailyFXRate Error:', { message: error.message, baseCurrency, settlementCurrency });
      throw error;
    }
  }

  async verifyPaymentStatus(transactionId) {
    try {
      const response = await this.client.post('/verifypaymentstatus', {
        EntityTransactionID: transactionId
      });
      return response.data;
    } catch (error) {
      logger.error('Billers verifyPaymentStatus Error:', { message: error.message, transactionId });
      throw error;
    }
  }
}

module.exports = new BillersProvider();
