const { Worker } = require("bullmq");
const Product = require("../models/product.model");
const { connection } = require("../config/bullmq");
const { bulkInsertProducts } = require("../services/bulkImport.service");
const { PRODUCT_QUEUE_NAME } = require("../queues/product.queue");

const processProductJob = async (job) => {
  const { name, data } = job;

  switch (name) {
    case "create": {
      const { name: productName, price, description } = data;
      return Product.create({
        name: productName,
        price,
        description,
      });
    }
    case "update": {
      const { id, name: productName, price, description } = data;
      const product = await Product.findByIdAndUpdate(
        id,
        { name: productName, price, description },
        { new: true, runValidators: true },
      );
      if (!product) {
        throw new Error("Product not found");
      }
      return product;
    }
    case "delete": {
      const { id } = data;
      const product = await Product.findByIdAndDelete(id);
      if (!product) {
        throw new Error("Product not found");
      }
      return product;
    }
    case "bulkCreate": {
      const { products } = data;
      return bulkInsertProducts(products, (progress) =>
        job.updateProgress(progress),
      );
    }
    case "bulkDelete": {
      const { deletedCount } = await Product.deleteMany({});
      return { deletedCount };
    }
    default:
      throw new Error(`Unknown job type: ${name}`);
  }
};

const productWorker = new Worker(PRODUCT_QUEUE_NAME, processProductJob, {
  connection,
  concurrency: 5,
  lockDuration: 300000,
});

productWorker.on("failed", (job, err) => {
  console.error(`Product job ${job?.id} failed:`, err.message);
});

productWorker.on("completed", (job) => {
  console.log(`Product job ${job.id} completed`);
});

module.exports = productWorker;
