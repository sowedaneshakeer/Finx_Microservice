const authService = require('../../../services/giftcard/auth.service');
const logger = require('../../../utils/logger');

const login = (req, res) => {
  const { username, password } = req.body;

  const result = authService.login(username, password);
  if (result) {
    return res.json(result);
  }

  logger.warn(`Failed login attempt for user: ${username}`);
  res.status(401).json({ success: false, error: 'INVALID_CREDENTIALS' });
};

module.exports = { login };
