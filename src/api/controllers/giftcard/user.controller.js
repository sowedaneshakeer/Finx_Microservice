const userService = require('../../../services/giftcard/user.service');

const getUserDetails = async (req, res) => {
  try {
    const user = await userService.getUserDetails();
    if (!user) {
      return res.status(404).json({ success: false, error: 'USER_NOT_FOUND' });
    }
    res.json({ success: true, data: user });
  } catch (error) {
    res.status(500).json({ success: false, error: 'SERVER_ERROR' });
  }
};

module.exports = { getUserDetails };
