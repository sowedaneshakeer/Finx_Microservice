const config = require('../config');
const logger = require('../utils/logger');

const authenticate = (req, res, next) => {
  if (!config.apiKeys.enabled) {
    return next();
  }

  const apiKey = req.headers[config.apiKeys.header.toLowerCase()];

  if (!apiKey) {
    logger.warn('API key missing', { path: req.path, ip: req.ip });
    return res.status(401).json({ error: 'Unauthorized', message: 'API key is required' });
  }

  if (!config.apiKeys.keys.includes(apiKey)) {
    logger.warn('Invalid API key attempt', { path: req.path, ip: req.ip });
    return res.status(403).json({ error: 'Forbidden', message: 'Invalid API key' });
  }

  next();
};

module.exports = authenticate;
