import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import FreebieBanner from "../models/freebieBanner.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all freebieBanners
export const getAllFreebieBanners = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(FreebieBanner.find(), req.query)
    .filter()
    .sort()
    .limitFields();

  const freebieBanners = await features.query;

  // Return the freebieBanner
  res.status(200).json({ freebieBanners });
});

// Function to get a single freebieBanner
export const getFreebieBanner = catchAsync(async (req, res, next) => {
  const freebieBannerId = req.params.id;

  // Find the freebieBanner by id
  const freebieBanner = await FreebieBanner.findById(freebieBannerId);

  // Check if the freebieBanner exists
  if (!freebieBanner) {
    return next(new AppError("FreebieBanner not found", 404));
  }

  // Return the freebieBanner
  res.status(200).json({ freebieBanner });
});

// Function to create a freebieBanner
export const createFreebieBanner = catchAsync(async (req, res, next) => {
  req.body.image = req.file.filename;
  const freebieBanner = await FreebieBanner.create(req.body);
  // Return the created freebieBanner
  res.status(201).json({ freebieBanner });
});

// Function to update a freebieBanner
export const updateFreebieBanner = catchAsync(async (req, res, next) => {
  const freebieBannerId = req.params.id;

  if (req.file) {
    req.body.image = req.file.filename;
  }

  // Find the freebieBanner by id and update the details
  const freebieBanner = await FreebieBanner.findByIdAndUpdate(
    freebieBannerId,
    req.body,
    {
      new: true,
      runValidators: true,
    }
  );

  // Check if the freebieBanner exists
  if (!freebieBanner) {
    return next(new AppError("FreebieBanner not found", 404));
  }

  // Return the updated freebieBanner
  res.status(200).json({ freebieBanner });
});

// Function to delete a freebieBanner
export const deleteFreebieBanner = catchAsync(async (req, res, next) => {
  const freebieBannerId = req.params.id;

  // Find the freebieBanner by id and delete it
  const freebieBanner = await FreebieBanner.findByIdAndUpdate(
    freebieBannerId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the freebieBanner exists
  if (!freebieBanner) {
    return next(new AppError("FreebieBanner not found", 404));
  }

  // Return the deleted freebieBanner
  res.status(204).json({ freebieBanner });
});
