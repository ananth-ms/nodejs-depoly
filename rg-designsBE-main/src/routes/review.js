import express from "express";
import {
  getAllReviews,
  getReview,
  createReview,
  updateReview,
  deleteReview,
} from "../controllers/review.js";
import { protect, restrictTo } from "../controllers/user.js";

// Create a new router instance
const reviewRouter = express.Router();

reviewRouter.route("/").get(getAllReviews).post(protect, createReview);

reviewRouter
  .route("/:id")
  .get(getReview)
  .patch(protect, updateReview)
  .delete(restrictTo("admin"), deleteReview);

// Export the router
export default reviewRouter;
