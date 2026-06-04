const app = require("./app");
const connectDB = require("./config/db");

connectDB().catch((err) => console.error("Mongo DB connection error", err));

require("./workers/product.worker");

const port = process.env.PORT || 5000;

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
