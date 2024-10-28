import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import Coupon from "../models/coupon.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all coupons
export const getAllCoupons = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(Coupon.find(), req.query)
    .filter()
    .sort()
    .limitFields();

  const coupons = await features.query;

  // Return the coupon
  res.status(200).json({ coupons });
});

// Function to get a single coupon
export const getCoupon = catchAsync(async (req, res, next) => {
  const couponId = req.params.id;

  // Find the coupon by id
  const coupon = await Coupon.findById(couponId);

  // Check if the coupon exists
  if (!coupon) {
    return next(new AppError("Coupon not found", 404));
  }

  // Return the coupon
  res.status(200).json({ coupon });
});

// Function to create a coupon
export const createCoupon = catchAsync(async (req, res, next) => {
  req.body.image = req.file.filename;
  const coupon = await Coupon.create(req.body);
  // Return the created coupon
  res.status(201).json({ coupon });
});

// Function to update a coupon
export const updateCoupon = catchAsync(async (req, res, next) => {
  const couponId = req.params.id;

  if (req.file) {
    req.body.image = req.file.filename;
  }

  // Find the coupon by id and update the details
  const coupon = await Coupon.findByIdAndUpdate(couponId, req.body, {
    new: true,
    runValidators: true,
  });

  // Check if the coupon exists
  if (!coupon) {
    return next(new AppError("Coupon not found", 404));
  }

  // Return the updated coupon
  res.status(200).json({ coupon });
});

// Function to delete a coupon
export const deleteCoupon = catchAsync(async (req, res, next) => {
  const couponId = req.params.id;

  // Find the coupon by id and delete it
  const coupon = await Coupon.findByIdAndUpdate(
    couponId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the coupon exists
  if (!coupon) {
    return next(new AppError("Coupon not found", 404));
  }

  // Return the deleted coupon
  res.status(204).json({ coupon });
});
