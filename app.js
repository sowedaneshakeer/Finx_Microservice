require('dotenv').config();
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const bodyParser = require('body-parser');
const rateLimit = require('express-rate-limit');
const logger = require('./src/utils/logger');

// Import routes
const routes = require('./src/api/routes');
const giftcardRoutes = require('./src/api/routes/giftcard');

const app = express();

// Middleware
app.use(helmet());
app.use(cors());
app.use(bodyParser.json());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Rate limiting
const limiter = rateLimit({
  windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS) || 900000,
  max: parseInt(process.env.RATE_LIMIT_MAX) || 100,
  message: { success: false, error: 'TOO_MANY_REQUESTS' }
});
app.use(limiter);

// Routes
app.use('/api/v1/giftcards', routes);
//app.use('/api/v1/giftcards', giftcardRoutes);

// Health check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'okss', service: 'products-microservice' });
});

// Error handling
app.use((err, req, res, next) => {
  logger.error('Unhandled error:', err);
  res.status(500).json({ success: false, error: 'SERVER_ERROR' });
});

const PORT = process.env.PORT || 3009;
app.listen(PORT, () => {
  logger.info(`Products microservice running on port ${PORT}`);
});
