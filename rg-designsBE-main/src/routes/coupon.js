import express from "express";
import {
  getAllCoupons,
  getCoupon,
  createCoupon,
  updateCoupon,
  deleteCoupon,
} from "../controllers/coupon.js";
import { protect, restrictTo } from "../controllers/user.js";
import { uploadImage } from "../utils/multerConfig.js";
const couponRouter = express.Router();

couponRouter
  .route("/")
  .get(getAllCoupons)
  .post(protect, restrictTo("admin"), uploadImage, createCoupon);

couponRouter
  .route("/:id")
  .get(getCoupon)
  .patch(protect, restrictTo("admin"), uploadImage, updateCoupon)
  .delete(protect, restrictTo("admin"), deleteCoupon);

export default couponRouter;
