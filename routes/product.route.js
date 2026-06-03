const express = require("express");
const router = express.Router();
const {
  createProduct,
  updateProduct,
  deleteProduct,
  getAllProducts,
  bulkCreateProducts,
  bulkDeleteProducts,
} = require("../controller/product.controller");

router.get("/", getAllProducts);
router.post("/bulk", bulkCreateProducts);
router.delete("/bulk", bulkDeleteProducts);
router.post("/", createProduct);
router.put("/:id", updateProduct);
router.delete("/:id", deleteProduct);
module.exports = router;
