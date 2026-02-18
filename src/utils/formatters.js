function formatUserResponse(user) {
  return {
    id: user.agent_id,
    email: user.email,
    name: `${user.first_name} ${user.last_name}`,
    company: user.company,
    phone: user.phone,
    address: {
      country: user.country?.name,
      city: user.city,
      state: user.state,
      zip: user.zip
    },
    balance: {
      usd: user.available_credit_usd,
      local: user.available_credit_local
    },
    wallets: user.wallets?.map(wallet => ({
      id: wallet.id,
      currency: wallet.currency_code,
      balance: wallet.available_credit,
      limit: wallet.credit_limit
    })),
    lastLogin: user.last_login_time,
    managerId: user.manager_id
  };
}

module.exports = { formatUserResponse };
