const express = require('express');
const router = express.Router();

const authRoutes = require('./auth.routes');
const countryRoutes = require('./country.routes');
const productRoutes = require('./product.routes');
const transactionRoutes = require('./transaction.routes');
const currencyRoutes = require('./currency.routes');
const catalogueRoutes = require('./catalogue.routes');
const userRoutes = require('./user.routes');

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
