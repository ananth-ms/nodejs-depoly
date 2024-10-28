import express from "express";
import {
  getAllReviews,
  getReview,
  createReview,
  updateReview,
  deleteReview,
} from "../controllers/customReview.js";
import { protect, restrictTo } from "../controllers/user.js";

// Create a new router instance
const customReviewRouter = express.Router();

customReviewRouter.route("/").get(getAllReviews).post(protect, createReview);

customReviewRouter
  .route("/:id")
  .get(getReview)
  .patch(protect, updateReview)
  .delete(restrictTo("admin"), deleteReview);

// Export the router
export default customReviewRouter;
