const http = require('http');
const options = {
  hostname: '127.0.0.1', port: 3009,
  path: '/api/v1/giftcards/products/dtone_56038',
  headers: { 'Content-Type': 'application/json', 'x-api-key': '04d0c681d154d1d5ecef623eab52099d258e21ab0d51af4e0b05e73aee7a41f1' }
};
http.get(options, (res) => {
  let body = '';
  res.on('data', chunk => body += chunk);
  res.on('end', () => {
    const data = JSON.parse(body);
    console.log(JSON.stringify(data, null, 2));
  });
}).on('error', err => console.error('Error:', err.message));
