import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import CustomReview from "../models/customReview.js";
import CustomOrder from "../models/customOrder.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all customReviews
export const getAllReviews = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(CustomReview.find(), req.query)
    .filter()
    .sort()
    .limitFields();

  const customReviews = await features.query
    .populate({
      path: "user",
      select: "name email phone",
    })
    .populate({
      path: "customOrder",
      select: "image description",
    });

  // Calculate the average rating
  const ratings = customReviews.map((review) => review.rating);
  const averageRating =
    ratings.reduce((acc, rating) => acc + rating, 0) / ratings.length;

  // Calculate the number of buyers count from customOrders collection
  const buyersCount = await CustomOrder.countDocuments({ isPaid: true });

  // Return the customReviews
  res.status(200).json({ customReviews, averageRating, buyersCount });
});

// Function to get a single customReview
export const getReview = catchAsync(async (req, res, next) => {
  const reviewId = req.params.id;

  // Find the customReview by id
  const customReview = await CustomReview.findById(reviewId);

  // Check if the customReview exists
  if (!customReview) {
    return next(new AppError("CustomReview not found", 404));
  }

  // Return the customReview
  res.status(200).json({ customReview });
});

// Function to create a customReview
export const createReview = catchAsync(async (req, res) => {
  const customOrder = await CustomOrder.findById(req.body.customOrder);

  if (!customOrder) {
    return next(new AppError("CustomOrder not found", 404));
  }
  req.body.category = customOrder.category;
  const customReview = await CustomReview.create(req.body);

  // Return the created customReview
  res.status(201).json({ customReview });
});

// Function to update a customReview
export const updateReview = catchAsync(async (req, res, next) => {
  const reviewId = req.params.id;

  // Find the customReview by id and update the details
  const customReview = await CustomReview.findByIdAndUpdate(
    reviewId,
    req.body,
    {
      new: true,
      runValidators: true,
    }
  );

  // Check if the customReview exists
  if (!customReview) {
    return next(new AppError("CustomReview not found", 404));
  }

  // Return the updated customReview
  res.status(200).json({ customReview });
});

// Function to delete a customReview
export const deleteReview = catchAsync(async (req, res, next) => {
  const reviewId = req.params.id;

  // Find the customReview by id and delete it
  const customReview = await CustomReview.findByIdAndUpdate(
    reviewId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the customReview exists
  if (!customReview) {
    return next(new AppError("CustomReview not found", 404));
  }

  // Return the deleted customReview
  res.status(204).json({ customReview });
});
