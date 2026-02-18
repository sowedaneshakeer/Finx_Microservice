const jwt = require('jsonwebtoken');

const login = (username, password) => {
  if (username === 'admin' && password === 'password') {
    const token = jwt.sign(
      { username, role: 'admin' },
      process.env.JWT_SECRET || 'default-secret',
      { expiresIn: '1h' }
    );
    return { success: true, token };
  }
  return null;
};

module.exports = { login };
