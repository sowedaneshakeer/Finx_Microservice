const ENDPOINTS = {
  COUNTRIES: '/country/search-countries',
  PRODUCTS: '/product/search-all-products',
  TRANSACTION_CREATE: '/transaction/do-by-product',
  TRANSACTION_SEARCH: '/transaction/search-transactions',
  CURRENCIES: '/currency/search-currencies',
  CATALOGUE: '/catalogue/search-catalogue',
  USER: '/user',
};

const ERROR_CODES = {
  0: { code: 'INTERNAL_ERROR', message: 'Internal server error' },
  2: { code: 'TRANSACTION_FAILED', message: null },
  202: { code: 'OUT_OF_STOCK', message: 'Product out of stock' },
  204: { code: 'PRODUCT_UNAVAILABLE', message: 'Product currently unavailable' },
  211: { code: 'INSUFFICIENT_BALANCE_MA', message: 'Master account balance insufficient' },
  212: { code: 'INSUFFICIENT_BALANCE', message: 'Account balance insufficient' },
  301: { code: 'ACCOUNT_BLOCKED', message: 'Account access blocked' },
  311: { code: 'IP_UNAUTHORIZED', message: 'Request from unauthorized IP' },
};

module.exports = { ENDPOINTS, ERROR_CODES };
