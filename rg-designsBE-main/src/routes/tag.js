import express from "express";
import { getAllTags, getTag, createTag, updateTag, deleteTag } from "../controllers/tag.js";
import { protect, restrictTo } from "../controllers/user.js";

// Create a new router instance
const tagRouter = express.Router();

tagRouter.route("/").get(getAllTags).post(protect, createTag);

tagRouter.route("/:id").get(getTag).patch(protect, updateTag).delete(restrictTo("admin"), deleteTag);

// Export the router
export default tagRouter;
