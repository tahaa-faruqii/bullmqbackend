// const Product = require("../models/product.model");
// const { bulkInsertBatchSize } = require("../config/limits");

// async function bulkInsertProducts(products, onProgress) {
//   let createdCount = 0;
//   const failed = [];

//   for (let i = 0; i < products.length; i += bulkInsertBatchSize) {
//     const batch = products.slice(i, i + bulkInsertBatchSize).map((item) => ({
//       name: item.name,
//       price: item.price,
//       description: item.description ?? "",
//     }));

//     try {
//       const inserted = await Product.insertMany(batch, { ordered: false });
//       createdCount += inserted.length;
//     } catch (err) {
//       if (err.insertedDocs?.length) {
//         createdCount += err.insertedDocs.length;
//       }
//       if (err.writeErrors?.length) {
//         for (const writeErr of err.writeErrors) {
//           failed.push({
//             name: batch[writeErr.index]?.name,
//             message: writeErr.errmsg || writeErr.err?.message,
//           });
//         }
//       } else if (!err.insertedDocs?.length) {
//         for (const item of batch) {
//           failed.push({ name: item.name, message: err.message });
//         }
//       }
//     }

//     if (typeof onProgress === "function") {
//       await onProgress(
//         Math.min(100, Math.round(((i + batch.length) / products.length) * 100)),
//       );
//     }
//   }

//   return { createdCount, failed };
// }

// module.exports = { bulkInsertProducts };

const Product = require("../models/product.model");
const { bulkInsertBatchSize } = require("../config/limits");

async function bulkInsertProducts(products = [], onProgress) {
  if (!Array.isArray(products) || products.length === 0) {
    return { createdCount: 0, failed: [] };
  }

  let createdCount = 0;
  let processedCount = 0;
  const failed = [];

  const total = products.length;
  const chunkSize = Math.max(1, bulkInsertBatchSize || 10);

  for (let i = 0; i < total; i += chunkSize) {
    const rawBatch = products.slice(i, i + chunkSize);

    // Basic sanitization & validation
    const batch = [];
    for (const item of rawBatch) {
      if (!item?.name || typeof item.price !== "number") {
        failed.push({
          name: item?.name || "unknown",
          message: "Invalid product data (name and price required)",
        });
        continue;
      }

      batch.push({
        name: item.name.trim(),
        price: item.price,
        description: item.description ?? "",
      });
    }

    if (batch.length > 0) {
      try {
        const insertedDocs = await Product.insertMany(batch, {
          ordered: false,
          lean: true,
        });

        createdCount += insertedDocs.length;
      } catch (err) {
        // Handle partial success
        if (err?.writeErrors?.length) {
          const errorIndexes = new Set(err.writeErrors.map((e) => e.index));

          // Successful inserts
          createdCount += batch.length - errorIndexes.size;

          // Failed inserts
          for (const writeErr of err.writeErrors) {
            failed.push({
              name: batch[writeErr.index]?.name,
              message:
                writeErr.err?.message || writeErr.errmsg || "Insert failed",
            });
          }
        } else {
          // Entire batch failed
          for (const item of batch) {
            failed.push({
              name: item.name,
              message: err.message || "Batch insert failed",
            });
          }
        }
      }
    }

    processedCount += rawBatch.length;

    // Progress update
    if (typeof onProgress === "function") {
      const progress = Math.min(
        100,
        Math.round((processedCount / total) * 100),
      );

      await onProgress(progress);
    }
  }

  return {
    total,
    createdCount,
    failedCount: failed.length,
    failed,
  };
}

module.exports = { bulkInsertProducts };
