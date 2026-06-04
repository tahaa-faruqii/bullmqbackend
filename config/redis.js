const { Redis } = require("@upstash/redis");

/** HTTP REST client — same Upstash database, for direct Redis calls outside BullMQ */
const redis =
  process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN
    ? new Redis({
        url: process.env.UPSTASH_REDIS_REST_URL,
        token: process.env.UPSTASH_REDIS_REST_TOKEN,
      })
    : null;

module.exports = redis;
