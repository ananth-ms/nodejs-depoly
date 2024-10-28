import "dotenv/config";
import mongoose from "mongoose";

// Handle unhandled events
process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  process.exit(1);
});

import app from "./app.js";

// Connect to the database
const DB_URI = process.env.DB_URI;
const PORT = process.env.PORT;

mongoose
  .connect(DB_URI)
  .then(() => {
    console.log("Connected to the database");
    // Start the server after successful database connection
    app.listen(PORT, () => {
      console.log(`Server is listening on port ${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Database connection failed:", error);
  });

// Handle unhandled promise rejections
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection:", reason);
  process.exit(1);
});
