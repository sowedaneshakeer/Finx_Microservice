// const express = require('express');
// const router = express.Router();

// const productRoutes = require('./product.routes');

// // Mount product routes directly - GET / goes to product controller
// router.use('/', productRoutes);

// module.exports = router;

const express = require('express');
const router = express.Router();

const authRoutes = require('./giftcard/auth.routes');
const countryRoutes = require('./giftcard/country.routes');
const productRoutes = require('./giftcard/product.routes');
const transactionRoutes = require('./giftcard/transaction.routes');
const currencyRoutes = require('./giftcard/currency.routes');
const catalogueRoutes = require('./giftcard/catalogue.routes');
const userRoutes = require('./giftcard/user.routes');

router.get('/', (req, res) => {
  res.json({
    success: true,
    data: {
      service: 'Products Microservice - Gift Card API',
      version: 'v1',
      endpoints: [
        'GET /countries',
        'GET /countries/:iso',
        'GET /products',
        'GET /products/:id',
        'POST /transactions',
        'GET /transactions',
        'GET /transactions/:id',
        'GET /currencies',
        'GET /currencies/:code',
        'GET /catalouge',
        'GET /user',
        'POST /auth/login'
      ]
    }
  });
});

router.use('/auth', authRoutes);
router.use('/countries', countryRoutes);
router.use('/products', productRoutes);
router.use('/transactions', transactionRoutes);
router.use('/currencies', currencyRoutes);
router.use('/catalouge', catalogueRoutes);
router.use('/user', userRoutes);

module.exports = router;
