const mongoose = require("mongoose");

const DB_UNAVAILABLE = "Database unavailable. Try again later.";

const connectDB = async () => {
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log("MOngo DB successfully");
  } catch (error) {
    console.log("Mongo DB connection error", error);
    process.exit(1);
  }
};

function isDbConnected() {
  return mongoose.connection.readyState === 1;
}

function resolveDbError(error) {
  if (
    !isDbConnected() ||
    /buffering timed out|ECONNREFUSED|ENOTFOUND|MongoNetworkError/i.test(
      error?.message || "",
    )
  ) {
    return DB_UNAVAILABLE;
  }
  return error.message;
}

module.exports = connectDB;
module.exports.DB_UNAVAILABLE = DB_UNAVAILABLE;
module.exports.isDbConnected = isDbConnected;
module.exports.resolveDbError = resolveDbError;
