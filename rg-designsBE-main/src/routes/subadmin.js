import express from "express";
import {
  login,
  createSubadmin,
  logout,
  updatePassword,
  subadminProtect,
  getCurrentSubadmin,
  getAllSubadmins,
  getSubadmin,
  updateSubadmin,
  deleteSubadmin,
} from "../controllers/subadmin.js";
import { protect, restrictTo } from "../controllers/user.js";

// Create a new router instance
const subadminRouter = express.Router();

subadminRouter.route("/").get(protect, restrictTo("admin"), getAllSubadmins);
subadminRouter.route("/getMe").get(subadminProtect, getCurrentSubadmin);

// Auth routes
subadminRouter
  .route("/auth/create")
  .post(protect, restrictTo("admin"), createSubadmin);

subadminRouter.route("/auth/login").post(login);
subadminRouter.route("/auth/logout").post(logout);
subadminRouter
  .route("/auth/updatePassword/:id")
  .patch(protect, restrictTo("admin"), updatePassword);

subadminRouter
  .route("/:id")
  .get(protect, restrictTo("admin"), getSubadmin)
  .patch(protect, restrictTo("admin"), updateSubadmin)
  .delete(protect, restrictTo("admin"), deleteSubadmin);

// Export the router
export default subadminRouter;
