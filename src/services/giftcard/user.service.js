const globetopper = require('../../providers/globetopper.provider');
const dtone = require('../../providers/dtone.provider');
const billers = require('../../providers/billers.provider');
const ppn = require('../../providers/ppn.provider');
const { formatUserResponse } = require('../../utils/formatters');
const logger = require('../../utils/logger');

const getUserDetails = async () => {
  const user = await globetopper.getUserDetails();
  if (!user) return null;
  return formatUserResponse(user);
};

const getDtoneBalance = async () => {
  try {
    const balances = await dtone.getBalances();
    // DT-One returns an array of balance objects
    return { success: true, provider: 'DTONE', data: balances };
  } catch (error) {
    logger.error('Failed to fetch DT-One balance:', { message: error.message });
    throw error;
  }
};

const getBillersBalance = async () => {
  try {
    const balance = await billers.getBalance();
    return { success: true, provider: 'BILLERS', data: balance };
  } catch (error) {
    logger.error('Failed to fetch Billers balance:', { message: error.message });
    throw error;
  }
};

const getPpnBalance = async () => {
  try {
    const balance = await ppn.getBalance();
    return { success: true, provider: 'PPN', data: balance };
  } catch (error) {
    logger.error('Failed to fetch PPN balance:', { message: error.message });
    throw error;
  }
};

module.exports = { getUserDetails, getDtoneBalance, getBillersBalance, getPpnBalance };
