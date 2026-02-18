const globetopper = require('../../providers/globetopper.provider');
const { formatUserResponse } = require('../../utils/formatters');

const getUserDetails = async () => {
  const user = await globetopper.getUserDetails();
  if (!user) return null;
  return formatUserResponse(user);
};

module.exports = { getUserDetails };
