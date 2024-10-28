import express from "express";
import {
  getAllTransactions,
  getTransaction,
  createTransaction,
  updateTransaction,
  deleteTransaction,
} from "../controllers/transaction.js";
import { protect, restrictTo } from "../controllers/user.js";

// Create a new router instance
const transactionRouter = express.Router();

// CRUD routes - Protected
transactionRouter.use(protect);
transactionRouter.route("/").get(getAllTransactions).post(createTransaction);

transactionRouter
  .route("/:id")
  .get(getTransaction)
  .patch(restrictTo("admin"), updateTransaction)
  .delete(restrictTo("admin"), deleteTransaction);

// Export the router
export default transactionRouter;
