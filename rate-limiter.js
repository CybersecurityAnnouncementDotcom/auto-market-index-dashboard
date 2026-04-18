/**
 * Zero-dependency in-memory rate limiter for Express.
 * Tracks request counts per IP in a sliding window.
 * 
 * Usage:
 *   const { rateLimiter } = require('./rate-limiter');
 *   app.use('/api/', rateLimiter({ windowMs: 60000, max: 60 }));
 *   app.use('/api/export/', rateLimiter({ windowMs: 60000, max: 5 }));
 */

function rateLimiter({ windowMs = 60000, max = 60, message = 'Too many requests, please try again later.' } = {}) {
  const hits = new Map(); // ip -> { count, resetTime }

  // Cleanup expired entries every 5 minutes to prevent memory leaks
  setInterval(() => {
    const now = Date.now();
    for (const [ip, record] of hits) {
      if (now > record.resetTime) hits.delete(ip);
    }
  }, 5 * 60 * 1000).unref(); // .unref() so it doesn't keep the process alive

  return (req, res, next) => {
    const ip = req.headers['x-real-ip'] || req.ip || req.connection.remoteAddress;
    const now = Date.now();

    let record = hits.get(ip);
    if (!record || now > record.resetTime) {
      record = { count: 0, resetTime: now + windowMs };
      hits.set(ip, record);
    }

    record.count++;

    // Set rate limit headers
    res.setHeader('X-RateLimit-Limit', max);
    res.setHeader('X-RateLimit-Remaining', Math.max(0, max - record.count));
    res.setHeader('X-RateLimit-Reset', Math.ceil(record.resetTime / 1000));

    if (record.count > max) {
      res.status(429).json({ error: message });
      return;
    }

    next();
  };
}

module.exports = { rateLimiter };
