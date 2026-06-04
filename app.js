require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { jsonBodyLimit } = require("./config/limits");
const { ensureDbConnected, DB_UNAVAILABLE } = require("./config/db");

const app = express();
app.use(cors());
app.use(express.json({ limit: jsonBodyLimit }));

app.use(async (_req, res, next) => {
  try {
    await ensureDbConnected();
    next();
  } catch (error) {
    console.error("Database connection failed:", error.message);
    res.status(503).json({ message: DB_UNAVAILABLE });
  }
});

app.use("/api/products", require("./routes/product.route"));

app.use((err, _req, res, next) => {
  if (err.type === "entity.too.large") {
    return res.status(413).json({
      message: `Request body too large. Max size is ${jsonBodyLimit}. Split into smaller files and try again.`,
      limit: jsonBodyLimit,
      code: "PAYLOAD_TOO_LARGE",
    });
  }
  next(err);
});

module.exports = app;
