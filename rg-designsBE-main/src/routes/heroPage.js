import express from "express";
import {
  getAllHeroPages,
  getHeroPage,
  createHeroPage,
  updateHeroPage,
  deleteHeroPage,
} from "../controllers/heroPage.js";
import { protect, restrictTo } from "../controllers/user.js";
import { uploadImage } from "../utils/multerConfig.js";
const heroPageRouter = express.Router();

heroPageRouter.route("/").get(getAllHeroPages).post(protect, restrictTo("admin"), uploadImage, createHeroPage);

heroPageRouter
  .route("/:id")
  .get(getHeroPage)
  .patch(protect, restrictTo("admin"), uploadImage, updateHeroPage)
  .delete(protect, restrictTo("admin"), deleteHeroPage);

export default heroPageRouter;
