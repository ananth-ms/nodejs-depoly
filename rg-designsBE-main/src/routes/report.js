import express from "express";
import getReport from "../controllers/report.js";
import { protect, restrictTo } from "../controllers/user.js";

// Create a new router instance
const reportRouter = express.Router();

// CRUD routes - Protected
reportRouter.use(protect);
reportRouter.route("/").get(restrictTo("admin"), getReport);

// Export the router
export default reportRouter;
