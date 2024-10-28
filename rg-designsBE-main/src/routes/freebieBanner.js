import express from "express";
import {
  getAllFreebieBanners,
  getFreebieBanner,
  createFreebieBanner,
  updateFreebieBanner,
  deleteFreebieBanner,
} from "../controllers/freebieBanner.js";
import { protect, restrictTo } from "../controllers/user.js";
import { uploadImage } from "../utils/multerConfig.js";
const freebieBannerRouter = express.Router();

freebieBannerRouter
  .route("/")
  .get(getAllFreebieBanners)
  .post(protect, restrictTo("admin"), uploadImage, createFreebieBanner);

freebieBannerRouter
  .route("/:id")
  .get(getFreebieBanner)
  .patch(protect, restrictTo("admin"), uploadImage, updateFreebieBanner)
  .delete(protect, restrictTo("admin"), deleteFreebieBanner);

export default freebieBannerRouter;
