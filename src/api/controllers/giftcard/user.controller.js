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

const getDtoneBalance = async (req, res) => {
  try {
    const result = await userService.getDtoneBalance();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, provider: 'DTONE', error: error.message });
  }
};

const getBillersBalance = async (req, res) => {
  try {
    const result = await userService.getBillersBalance();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, provider: 'BILLERS', error: error.message });
  }
};

const getPpnBalance = async (req, res) => {
  try {
    const result = await userService.getPpnBalance();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, provider: 'PPN', error: error.message });
  }
};

module.exports = { getUserDetails, getDtoneBalance, getBillersBalance, getPpnBalance };
