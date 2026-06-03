const Product = require("../models/product.model");
const { resolveDbError, DB_UNAVAILABLE } = require("../config/db");
const { bulkInsertProducts } = require("../services/bulkImport.service");
const {
  maxBulkProducts,
  listStreamThreshold,
  listStreamBatchSize,
} = require("../config/limits");

const handleError = (res, error) => {
  if (error.message === "Product not found") {
    return res.status(404).json({ message: error.message });
  }

  const message = resolveDbError(error);
  const status = message === DB_UNAVAILABLE ? 503 : 500;
  res.status(status).json({ message });
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

    const result = await bulkInsertProducts(products);
    res.status(201).json(result);
  } catch (error) {
    handleError(res, error);
  }
};

const bulkDeleteProducts = async (_req, res) => {
  try {
    const { deletedCount } = await Product.deleteMany({});
    res.status(200).json({
      message: `${deletedCount} product${deletedCount === 1 ? "" : "s"} deleted.`,
      deletedCount,
    });
  } catch (error) {
    handleError(res, error);
  }
};

module.exports = {
  createProduct,
  updateProduct,
  deleteProduct,
  getAllProducts,
  bulkCreateProducts,
  bulkDeleteProducts,
};
