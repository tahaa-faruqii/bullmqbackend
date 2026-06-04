const Product = require("../models/product.model");
const { Job } = require("bullmq");
const { resolveDbError, DB_UNAVAILABLE } = require("../config/db");
const { productQueue } = require("../queues/product.queue");
const {
  listStreamThreshold,
  listStreamBatchSize,
} = require("../config/limits");

const REDIS_UNAVAILABLE =
  "Queue service unavailable. Check Redis connection and try again.";

const CHUNK_SIZE = 10; // process 10 records per job
const MAX_BULK_LIMIT = 10000; // safety limit

const isRedisError = (error) =>
  /redis|upstash|ECONNREFUSED|ENOTFOUND|connect EPERM|Connection is closed|Queue timeout|REDIS_URL/i.test(
    error?.message || "",
  );

const QUEUE_ADD_TIMEOUT_MS = 15000;

async function addQueueJob(name, data, options = {}) {
  const addPromise = productQueue.add(name, data, options);
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(
      () =>
        reject(
          new Error(
            "Queue timeout. Add REDIS_URL (Upstash TCP rediss:// URL) on Railway.",
          ),
        ),
      QUEUE_ADD_TIMEOUT_MS,
    );
  });
  return Promise.race([addPromise, timeoutPromise]);
}

const handleError = (res, error) => {
  if (error.message === "Product not found") {
    return res.status(404).json({ message: error.message });
  }

  if (isRedisError(error)) {
    return res.status(503).json({ message: REDIS_UNAVAILABLE });
  }

  const message = resolveDbError(error);
  const status = message === DB_UNAVAILABLE ? 503 : 500;
  res.status(status).json({ message });
};

const handleQueueError = (res, error, jobId) => {
  if (isRedisError(error)) {
    return res.status(503).json({ message: REDIS_UNAVAILABLE });
  }

  if (error.message?.includes("timed out") && jobId) {
    return res.status(202).json({
      message: "Job is still processing",
      jobId,
      status: "processing",
    });
  }

  if (error.message === "Product not found") {
    return res.status(404).json({ message: error.message });
  }

  const message = resolveDbError(error);
  const status = message === DB_UNAVAILABLE ? 503 : 500;
  res.status(status).json({ message: error.message || message });
};

const createProduct = async (req, res) => {
  try {
    const { name, price, description } = req.body;
    const product = await Product.create({ name, price, description });
    res.status(201).json(product);
  } catch (error) {
    handleError(res, error);
  }
};

const updateProduct = async (req, res) => {
  try {
    const { id } = req.params;
    const { name, price, description } = req.body;
    const product = await Product.findByIdAndUpdate(
      id,
      { name, price, description },
      { new: true, runValidators: true },
    );

    if (!product) {
      return res.status(404).json({ message: "Product not found" });
    }

    res.status(200).json(product);
  } catch (error) {
    handleError(res, error);
  }
};

const deleteProduct = async (req, res) => {
  try {
    const { id } = req.params;
    const product = await Product.findByIdAndDelete(id);

    if (!product) {
      return res.status(404).json({ message: "Product not found" });
    }

    res.status(200).json(product);
  } catch (error) {
    handleError(res, error);
  }
};

const getAllProducts = async (req, res) => {
  try {
    const total = await Product.countDocuments();

    if (total <= listStreamThreshold) {
      const products = await Product.find().sort({ createdAt: -1 }).lean();
      return res.status(200).json(products);
    }

    const batchSize = Math.min(
      listStreamBatchSize,
      Math.max(50, Math.ceil(total / 50)),
    );

    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.status(200);

    const cursor = Product.find()
      .sort({ createdAt: -1 })
      .lean()
      .cursor({ batchSize });

    const closeCursor = () => {
      cursor.close().catch(() => {});
    };
    req.on("close", closeCursor);
    res.on("close", closeCursor);

    res.write("[");
    let first = true;

    for await (const doc of cursor) {
      if (res.writableEnded) break;
      if (!first) res.write(",");
      first = false;
      res.write(JSON.stringify(doc));
    }

    if (!res.writableEnded) {
      res.write("]");
      res.end();
    }
  } catch (error) {
    if (!res.headersSent) {
      handleError(res, error);
    } else if (!res.writableEnded) {
      res.end();
    }
  }
};

const bulkCreateProducts = async (req, res) => {
  try {
    const { products } = req.body;

    if (!Array.isArray(products)) {
      return res.status(400).json({
        message: "products must be an array",
        code: "INVALID_BODY",
      });
    }

    if (products.length === 0) {
      return res.status(400).json({
        message: "products array cannot be empty",
        code: "EMPTY_PRODUCTS",
      });
    }

    if (products.length > MAX_BULK_LIMIT) {
      return res.status(400).json({
        message: `Bulk import limit exceeded. Maximum allowed: ${MAX_BULK_LIMIT}`,
        code: "BULK_LIMIT_EXCEEDED",
      });
    }
    const total = products.length;
    const jobs = [];

    for (let i = 0; i < total; i += CHUNK_SIZE) {
      const chunk = products.slice(i, i + CHUNK_SIZE);

      const job = await addQueueJob(
        "bulkCreate",
        { products: chunk },
        {
          attempts: 3,
          backoff: { type: "exponential", delay: 3000 },
          removeOnComplete: { age: 86400 },
          removeOnFail: { age: 86400 },
        },
      );
      jobs.push(job.id);
    }

    return res.status(202).json({
      message: "Bulk import queued",
      totalRecords: total,
      totalChunks: jobs.length,
      chunkSize: CHUNK_SIZE,
      jobIds: jobs,
      status: "processing",
    });
  } catch (error) {
    handleQueueError(res, error);
  }
};

// const getBulkJobStatus = async (req, res) => {
//   try {
//     const { jobId } = req.params;

//     if (!jobId) {
//       return res.status(400).json({
//         message: "jobId is required",
//         code: "MISSING_JOB_ID",
//       });
//     }

//     const job = await Job.fromId(productQueue, jobId);

//     if (!job) {
//       return res.status(404).json({
//         message: "Job not found",
//         jobId,
//         code: "JOB_NOT_FOUND",
//       });
//     }

//     const state = await job.getState();

//     if (state === "completed") {
//       return res.status(200).json({
//         status: state,
//         jobId,
//         ...(job.returnvalue || {}),
//       });
//     }

//     if (state === "failed") {
//       return res.status(200).json({
//         status: state,
//         jobId,
//         message: job.failedReason || "Bulk import failed",
//         code: "JOB_FAILED",
//       });
//     }

//     const progress =
//       typeof job.progress === "number" ? job.progress : undefined;

//     return res.status(200).json({
//       status: state,
//       jobId,
//       progress,
//     });
//   } catch (error) {
//     handleQueueError(res, error);
//   }
// };

const getBulkJobStatus = async (req, res) => {
  try {
    const { jobId } = req.params;

    if (jobId) {
      const job = await Job.fromId(productQueue, jobId);

      if (!job) {
        return res.status(404).json({
          message: "Job not found",
          jobId,
          code: "JOB_NOT_FOUND",
        });
      }

      const state = await job.getState();

      if (state === "completed") {
        return res.status(200).json({
          status: state,
          jobId,
          ...(job.returnvalue || {}),
        });
      }

      if (state === "failed") {
        return res.status(200).json({
          status: state,
          jobId,
          message: job.failedReason || "Job failed",
          code: "JOB_FAILED",
        });
      }

      const progress =
        typeof job.progress === "number" ? job.progress : undefined;

      return res.status(200).json({ status: state, jobId, progress });
    }

    const { jobIds } = req.body;

    if (!Array.isArray(jobIds) || jobIds.length === 0) {
      return res.status(400).json({
        message: "jobIds must be a non-empty array",
        code: "INVALID_JOB_IDS",
      });
    }

    const results = [];

    for (const jobId of jobIds) {
      const job = await Job.fromId(productQueue, jobId);

      if (!job) {
        results.push({
          jobId,
          status: "not_found",
        });
        continue;
      }

      const state = await job.getState();

      results.push({
        jobId,
        status: state,
        progress: typeof job.progress === "number" ? job.progress : undefined,
        result: state === "completed" ? job.returnvalue : undefined,
        error: state === "failed" ? job.failedReason : undefined,
      });
    }

    return res.status(200).json({
      totalJobs: jobIds.length,
      jobs: results,
    });
  } catch (error) {
    handleQueueError(res, error);
  }
};

const bulkDeleteProducts = async (_req, res) => {
  try {
    const job = await addQueueJob(
      "bulkDelete",
      {},
      {
        attempts: 2,
        removeOnComplete: { age: 3600 },
        removeOnFail: { age: 86400 },
      },
    );

    return res.status(202).json({
      message: "Bulk delete queued",
      jobId: job.id,
      status: "processing",
    });
  } catch (error) {
    handleQueueError(res, error);
  }
};

// const bulkDeleteProducts = async (_req, res) => {
//   try {
//     const { deletedCount } = await Product.deleteMany({});
//     res.status(200).json({
//       message: `${deletedCount} product${deletedCount === 1 ? "" : "s"} deleted.`,
//       deletedCount,
//     });
//   } catch (error) {
//     handleError(res, error);
//   }
// };

module.exports = {
  createProduct,
  updateProduct,
  deleteProduct,
  getAllProducts,
  bulkCreateProducts,
  bulkDeleteProducts,
  getBulkJobStatus,
};
