import express from "express";
import {
  getAllCustomOrders,
  getCustomOrder,
  createCustomOrder,
  updateCustomOrder,
  deleteCustomOrder,
} from "../controllers/customOrder.js";
import { uploadImageAndZipOptional } from "../utils/multerConfig.js";
import { protect, restrictTo } from "../controllers/user.js";

// Create a new router instance
const customOrder = express.Router();

// CRUD routes - Protected
customOrder
  .route("/")
  .get(getAllCustomOrders)
  .post(protect, uploadImageAndZipOptional, createCustomOrder);

customOrder
  .route("/:id")
  .get(getCustomOrder)
  .patch(
    protect,
    restrictTo("admin"),
    uploadImageAndZipOptional,
    updateCustomOrder
  )
  .delete(protect, restrictTo("admin"), deleteCustomOrder);

// Export the router
export default customOrder;
