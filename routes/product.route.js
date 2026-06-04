const express = require("express");
const router = express.Router();
const {
  createProduct,
  updateProduct,
  deleteProduct,
  getAllProducts,
  bulkCreateProducts,
  bulkDeleteProducts,
  getBulkJobStatus,
} = require("../controller/product.controller");

app.get("/health", (req, res) => {
  res.status(200).json({
    status: "ok",
    message: "Server is running",
  });
});
router.get("/", getAllProducts);
router.get("/jobs/:jobId", getBulkJobStatus);
router.post("/bulk", bulkCreateProducts);
router.delete("/bulk", bulkDeleteProducts);
router.post("/", createProduct);
router.put("/:id", updateProduct);
router.delete("/:id", deleteProduct);
module.exports = router;
