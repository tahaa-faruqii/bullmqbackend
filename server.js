const app = require("./app");
const productRouter = require("./routes/product.route");
const connectDB = require("./config/db");
connectDB().catch((err) => console.error("Mongo DB connection error", err));

require("./workers/product.worker");

app.listen(process.env.PORT, () => {
  console.log(`Server is running on port ${process.env.PORT}`);
});
app.use("/api/products", productRouter);
