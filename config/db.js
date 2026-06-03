const mongoose = require("mongoose");

const DB_UNAVAILABLE = "Database unavailable. Try again later.";

let cached = global.mongoose;

if (!cached) {
  cached = global.mongoose = { conn: null, promise: null };
}

async function ensureDbConnected() {
  if (cached.conn && mongoose.connection.readyState === 1) {
    return cached.conn;
  }

  if (!process.env.MONGO_URI) {
    throw new Error("MONGO_URI is not set");
  }

  if (!cached.promise) {
    cached.promise = mongoose
      .connect(process.env.MONGO_URI, {
        serverSelectionTimeoutMS: 10000,
        bufferCommands: false,
      })
      .then((conn) => {
        cached.conn = conn;
        return conn;
      })
      .catch((err) => {
        cached.promise = null;
        throw err;
      });
  }

  await cached.promise;
  return cached.conn;
}

const connectDB = async () => {
  try {
    await ensureDbConnected();
    console.log("Mongo DB successfully");
  } catch (error) {
    console.log("Mongo DB connection error", error);
    if (!process.env.VERCEL) {
      process.exit(1);
    }
    throw error;
  }
};

function isDbConnected() {
  return mongoose.connection.readyState === 1;
}

function resolveDbError(error) {
  if (
    !isDbConnected() ||
    /buffering timed out|ECONNREFUSED|ENOTFOUND|MongoNetworkError|MONGO_URI is not set/i.test(
      error?.message || "",
    )
  ) {
    return DB_UNAVAILABLE;
  }
  return error.message;
}

module.exports = connectDB;
module.exports.DB_UNAVAILABLE = DB_UNAVAILABLE;
module.exports.ensureDbConnected = ensureDbConnected;
module.exports.isDbConnected = isDbConnected;
module.exports.resolveDbError = resolveDbError;
