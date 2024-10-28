import express from "express";

import userRouter from "./routes/user.js";
import productRouter from "./routes/product.js";
import transactionRouter from "./routes/transaction.js";
import reportRouter from "./routes/report.js";
import reviewRouter from "./routes/review.js";
import paymentRouter from "./routes/payment.js";
import couponRouter from "./routes/coupon.js";
import customOrder from "./routes/customOrder.js";
import customReviewRouter from "./routes/customReview.js";
import subadminRouter from "./routes/subadmin.js";
import customBannerRouter from "./routes/customBanner.js";
import tagRouter from "./routes/tag.js";

// CMS imports
import freebieBannerRouter from "./routes/freebieBanner.js";
import heroPageRouter from "./routes/heroPage.js";

const router = express.Router();

router.use("/payment", paymentRouter);

router.use("/user", userRouter);

router.use("/product", productRouter);

router.use("/transaction", transactionRouter);

router.use("/report", reportRouter);

router.use("/coupon", couponRouter);

router.use("/review", reviewRouter);

router.use("/customOrder", customOrder);

router.use("/customReview", customReviewRouter);

router.use("/subadmin", subadminRouter);

router.use("/customBanner", customBannerRouter);

router.use("/tag", tagRouter);

// CMS routes
router.use("/heroPage", heroPageRouter);
router.use("/freebieBanner", freebieBannerRouter);

// Export the router
export default router;
