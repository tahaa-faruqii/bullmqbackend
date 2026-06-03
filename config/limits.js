const maxBulkProducts = Number(process.env.MAX_BULK_PRODUCTS) || 10000;
const jsonBodyLimit = process.env.JSON_BODY_LIMIT || "1gb";

const listStreamThreshold =
  Number(process.env.LIST_STREAM_THRESHOLD) || 500;
const listStreamBatchSize =
  Number(process.env.LIST_STREAM_BATCH_SIZE) || 100;

const jobWaitMsBase = Number(process.env.JOB_WAIT_MS_BASE) || 30000;
const jobWaitMsPerProduct = Number(process.env.JOB_WAIT_MS_PER_PRODUCT) || 100;
const maxJobWaitMs = Number(process.env.MAX_JOB_WAIT_MS) || 300000;
const bulkInsertBatchSize =
  Number(process.env.BULK_INSERT_BATCH_SIZE) || 500;

function getBulkJobWaitMs(productCount) {
  const count = Math.max(1, Number(productCount) || 1);
  return Math.min(jobWaitMsBase + count * jobWaitMsPerProduct, maxJobWaitMs);
}

module.exports = {
  jsonBodyLimit,
  maxBulkProducts,
  listStreamThreshold,
  listStreamBatchSize,
  jobWaitMsBase,
  jobWaitMsPerProduct,
  maxJobWaitMs,
  getBulkJobWaitMs,
  bulkInsertBatchSize,
};
