const Product = require("../models/product.model");
const { bulkInsertBatchSize } = require("../config/limits");

async function bulkInsertProducts(products, onProgress) {
  let createdCount = 0;
  const failed = [];

  for (let i = 0; i < products.length; i += bulkInsertBatchSize) {
    const batch = products.slice(i, i + bulkInsertBatchSize).map((item) => ({
      name: item.name,
      price: item.price,
      description: item.description ?? "",
    }));

    try {
      const inserted = await Product.insertMany(batch, { ordered: false });
      createdCount += inserted.length;
    } catch (err) {
      if (err.insertedDocs?.length) {
        createdCount += err.insertedDocs.length;
      }
      if (err.writeErrors?.length) {
        for (const writeErr of err.writeErrors) {
          failed.push({
            name: batch[writeErr.index]?.name,
            message: writeErr.errmsg || writeErr.err?.message,
          });
        }
      } else if (!err.insertedDocs?.length) {
        for (const item of batch) {
          failed.push({ name: item.name, message: err.message });
        }
      }
    }

    if (typeof onProgress === "function") {
      await onProgress(
        Math.min(100, Math.round(((i + batch.length) / products.length) * 100)),
      );
    }
  }

  return { createdCount, failed };
}

module.exports = { bulkInsertProducts };
