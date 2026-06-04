const { Queue } = require("bullmq");
const { connection } = require("../config/bullmq");

const PRODUCT_QUEUE_NAME = "product";

const defaultJobOptions = {
  attempts: 3,
  backoff: {
    type: "exponential",
    delay: 2000,
  },
  removeOnComplete: { age: 86400 },
  removeOnFail: { age: 86400 },
};

const productQueue = new Queue(PRODUCT_QUEUE_NAME, {
  connection,
  defaultJobOptions,
});

module.exports = {
  productQueue,
  PRODUCT_QUEUE_NAME,
};
