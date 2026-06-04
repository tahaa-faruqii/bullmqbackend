const IORedis = require("ioredis");

/**
 * BullMQ + Upstash (official pattern)
 * https://upstash.com/docs/redis/integrations/bullmq
 *
 * Uses UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN from your Upstash console.
 * REST URL hostname = Redis endpoint; token = Redis password (TLS required).
 *
 * Do NOT pass { url, token } directly to BullMQ — it treats that as ioredis opts
 * and hangs (~5m → Railway 499/502).
 */
function createUpstashConnection() {
  const restUrl = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!restUrl || !token) {
    throw new Error(
      "UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN must be set for BullMQ",
    );
  }

  const host = new URL(restUrl).hostname;
  const port = Number(process.env.UPSTASH_REDIS_PORT || 6379);

  return new IORedis({
    host,
    port,
    password: token,
    tls: {},
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    connectTimeout: 10000,
    commandTimeout: 30000,
  });
}

const connection = createUpstashConnection();

module.exports = { connection, createUpstashConnection };
