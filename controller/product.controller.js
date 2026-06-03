const Product = require("../models/product.model");
const { Job } = require("bullmq");
const { resolveDbError, DB_UNAVAILABLE } = require("../config/db");
const { bulkInsertProducts } = require("../services/bulkImport.service");
const { productQueue, queueEvents } = require("../queues/product.queue");
const {
  maxBulkProducts,
  jobWaitMsBase,
  listStreamThreshold,
  listStreamBatchSize,
} = require("../config/limits");

const waitForJob = async (job, waitMs = jobWaitMsBase) => {
  return job.waitUntilFinished(queueEvents, waitMs);
};

const handleQueueError = (res, error, jobId) => {
  if (error.message?.includes("timed out") && jobId) {
    return res.status(202).json({
      message: "Request queued — high load. Job will complete shortly.",
      jobId,
      status: "processing",
    });
  }

  if (error.message === "Product not found") {
    return res.status(404).json({ message: error.message });
  }

  return res.status(500).json({ message: error.message });
};

const createProduct = async (req, res) => {
  let job;
  try {
    const { name, price, description } = req.body;
    job = await productQueue.add("create", { name, price, description });
    const product = await waitForJob(job);
    res.status(201).json(product);
  } catch (error) {
    handleQueueError(res, error, job?.id);
  }
};

const updateProduct = async (req, res) => {
  let job;
  try {
    const { id } = req.params;
    const { name, price, description } = req.body;
    job = await productQueue.add("update", { id, name, price, description });
    const product = await waitForJob(job);
    res.status(200).json(product);
  } catch (error) {
    handleQueueError(res, error, job?.id);
  }
};

const deleteProduct = async (req, res) => {
  let job;
  try {
    const { id } = req.params;
    job = await productQueue.add("delete", { id });
    const product = await waitForJob(job);
    res.status(200).json(product);
  } catch (error) {
    handleQueueError(res, error, job?.id);
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
      const message = resolveDbError(error);
      const status = message === DB_UNAVAILABLE ? 503 : 500;
      res.status(status).json({ message });
    } else if (!res.writableEnded) {
      res.end();
    }
  }
};

const bulkCreateProducts = async (req, res) => {
  try {
    const { products } = req.body;
    if (!Array.isArray(products) || products.length === 0) {
      return res.status(400).json({ message: "products array is required" });
    }

    if (products.length > maxBulkProducts) {
      return res.status(400).json({
        message: `Too many products (${products.length}). Maximum ${maxBulkProducts} per upload. Split your file and try again.`,
        maxProducts: maxBulkProducts,
        received: products.length,
      });
    }

    if (process.env.VERCEL) {
      const result = await bulkInsertProducts(products);
      return res.status(201).json(result);
    }

    const job = await productQueue.add("bulkCreate", { products });
    res.status(202).json({
      message: "Bulk import started",
      jobId: job.id,
      status: "processing",
      total: products.length,
    });
  } catch (error) {
    handleQueueError(res, error);
  }
};

const getBulkJobStatus = async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await Job.fromId(productQueue, jobId);

    if (!job) {
      return res.status(404).json({ message: "Job not found" });
    }

    const state = await job.getState();

    if (state === "completed") {
      return res.status(200).json({
        status: state,
        ...(job.returnvalue || {}),
      });
    }

    if (state === "failed") {
      return res.status(200).json({
        status: state,
        message: job.failedReason || "Bulk job failed",
      });
    }

    const progress =
      typeof job.progress === "number" ? job.progress : undefined;

    return res.status(200).json({ status: state, progress });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

const bulkDeleteProducts = async (_req, res) => {
  let job;
  try {
    job = await productQueue.add("bulkDelete", {});
    const result = await waitForJob(job);
    res.status(200).json({
      message: `${result.deletedCount} product${result.deletedCount === 1 ? "" : "s"} deleted.`,
      deletedCount: result.deletedCount,
    });
  } catch (error) {
    handleQueueError(res, error, job?.id);
  }
};

module.exports = {
  createProduct,
  updateProduct,
  deleteProduct,
  getAllProducts,
  bulkCreateProducts,
  bulkDeleteProducts,
  getBulkJobStatus,
};
