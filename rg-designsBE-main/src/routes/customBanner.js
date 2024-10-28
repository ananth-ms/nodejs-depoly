import express from "express";
import {
  getAllCustomBanners,
  getCustomBanner,
  createCustomBanner,
  updateCustomBanner,
  deleteCustomBanner,
} from "../controllers/customBanner.js";
import { protect, restrictTo } from "../controllers/user.js";
import { uploadImage } from "../utils/multerConfig.js";
const customBannerRouter = express.Router();

customBannerRouter
  .route("/")
  .get(getAllCustomBanners)
  .post(protect, restrictTo("admin"), uploadImage, createCustomBanner);

customBannerRouter
  .route("/:id")
  .get(getCustomBanner)
  .patch(protect, restrictTo("admin"), uploadImage, updateCustomBanner)
  .delete(protect, restrictTo("admin"), deleteCustomBanner);

export default customBannerRouter;
