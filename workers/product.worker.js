// const { Worker } = require("bullmq");
// const Product = require("../models/product.model");
// const { connection } = require("../config/bullmq");
// const { bulkInsertProducts } = require("../services/bulkImport.service");
// const { PRODUCT_QUEUE_NAME } = require("../queues/product.queue");

// (async () => {
//   try {
//     await ensureDbConnected();
//     console.log("Worker MongoDB connected");
//   } catch (err) {
//     console.error("Worker DB connection failed:", err.message);
//     process.exit(1);
//   }
// })();

// const processProductJob = async (job) => {
//   const { name, data } = job;

//   switch (name) {
//     case "create": {
//       const { name: productName, price, description } = data;
//       return Product.create({
//         name: productName,
//         price,
//         description,
//       });
//     }
//     case "update": {
//       const { id, name: productName, price, description } = data;
//       const product = await Product.findByIdAndUpdate(
//         id,
//         { name: productName, price, description },
//         { new: true, runValidators: true },
//       );
//       if (!product) {
//         throw new Error("Product not found");
//       }
//       return product;
//     }
//     case "delete": {
//       const { id } = data;
//       const product = await Product.findByIdAndDelete(id);
//       if (!product) {
//         throw new Error("Product not found");
//       }
//       return product;
//     }
//     case "bulkCreate": {
//       const { products } = data;
//       return bulkInsertProducts(products, (progress) =>
//         job.updateProgress(progress),
//       );
//     }
//     case "bulkDelete": {
//       const { deletedCount } = await Product.deleteMany({});
//       return { deletedCount };
//     }
//     default:
//       throw new Error(`Unknown job type: ${name}`);
//   }
// };

// const productWorker = new Worker(PRODUCT_QUEUE_NAME, processProductJob, {
//   connection,
//   concurrency: 2,
//   lockDuration: 3600000,
//   maxStalledCount: 3,
// });

// productWorker.on("failed", (job, err) => {
//   console.error(`Product job ${job?.id} failed:`, err.message);
// });

// productWorker.on("completed", (job) => {
//   console.log(`Product job ${job.id} completed`);
// });

// module.exports = productWorker;

const { Worker } = require("bullmq");
const Product = require("../models/product.model");
const { connection } = require("../config/bullmq");
const { bulkInsertProducts } = require("../services/bulkImport.service");
const { PRODUCT_QUEUE_NAME } = require("../queues/product.queue");
const { ensureDbConnected } = require("../config/db");

// Ensure DB connected when worker starts
(async () => {
  try {
    await ensureDbConnected();
    console.log("Worker MongoDB connected");
  } catch (err) {
    console.error("Worker DB connection failed:", err.message);
    process.exit(1);
  }
})();

const processProductJob = async (job) => {
  const { name, data } = job;

  try {
    switch (name) {
      // ---------------- CREATE ----------------
      case "create": {
        const { name: productName, price, description } = data || {};

        if (!productName || typeof price !== "number") {
          throw new Error("Invalid product data");
        }

        return await Product.create({
          name: productName.trim(),
          price,
          description: description ?? "",
        });
      }

      // ---------------- UPDATE ----------------
      case "update": {
        const { id, name: productName, price, description } = data || {};

        if (!id) throw new Error("Product ID required");

        const product = await Product.findByIdAndUpdate(
          id,
          {
            ...(productName && { name: productName.trim() }),
            ...(typeof price === "number" && { price }),
            ...(description !== undefined && { description }),
          },
          { new: true, runValidators: true },
        );

        if (!product) {
          throw new Error("Product not found");
        }

        return product;
      }

      // ---------------- DELETE ----------------
      case "delete": {
        const { id } = data || {};

        if (!id) throw new Error("Product ID required");

        const product = await Product.findByIdAndDelete(id);

        if (!product) {
          throw new Error("Product not found");
        }

        return { deleted: true, id };
      }

      // ---------------- BULK CREATE ----------------
      case "bulkCreate": {
        const { products } = data || {};

        if (!Array.isArray(products) || products.length === 0) {
          throw new Error("Products array required");
        }

        return await bulkInsertProducts(products, async (progress) => {
          await job.updateProgress(progress);
        });
      }

      // ---------------- BULK DELETE ----------------
      case "bulkDelete": {
        const { deletedCount } = await Product.deleteMany({});
        return { deletedCount };
      }

      default:
        throw new Error(`Unknown job type: ${name}`);
    }
  } catch (error) {
    console.error(`Job ${job.id} failed internally:`, error.message);
    throw error; // Important: rethrow so BullMQ marks job failed
  }
};

const productWorker = new Worker(PRODUCT_QUEUE_NAME, processProductJob, {
  connection,
  concurrency: 3, // Better concurrency
  lockDuration: 300000, // 5 minutes
  maxStalledCount: 2,
});

// ---------------- Worker Events ----------------

productWorker.on("completed", (job) => {
  console.log(`Product job ${job.id} completed`);
});

productWorker.on("failed", (job, err) => {
  console.error(`Product job ${job?.id} failed:`, err.message);
});

productWorker.on("error", (err) => {
  console.error("Worker error:", err);
});

module.exports = productWorker;
