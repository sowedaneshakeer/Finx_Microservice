require('dotenv').config();

module.exports = {
  apiKeys: {
    enabled: process.env.API_KEY_AUTH_ENABLED === 'true',
    keys: process.env.API_KEYS ? process.env.API_KEYS.split(',') : ['default-proxy-key-123'],
    header: process.env.API_KEY_HEADER || 'x-api-key'
  }
};
