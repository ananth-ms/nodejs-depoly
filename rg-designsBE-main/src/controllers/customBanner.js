import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import CustomBanner from "../models/customBanner.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all custom banners
export const getAllCustomBanners = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(CustomBanner.find(), req.query)
    .filter()
    .sort()
    .limitFields();

  const customBanners = await features.query;

  // Return the custom banners
  res.status(200).json({ customBanners });
});

// Function to get a single custom banner
export const getCustomBanner = catchAsync(async (req, res, next) => {
  const customBannerId = req.params.id;

  // Find the custom banner by id
  const customBanner = await CustomBanner.findById(customBannerId);

  // Check if the custom banner exists
  if (!customBanner) {
    return next(new AppError("Hero page not found", 404));
  }

  // Return the custom banner
  res.status(200).json({ customBanner });
});

// Function to create a custom banner
export const createCustomBanner = catchAsync(async (req, res, next) => {
  req.body.image = req.file.filename;
  const customBanner = await CustomBanner.create(req.body);
  // Return the created custom banner
  res.status(201).json({ customBanner });
});

// Function to update a custom banner
export const updateCustomBanner = catchAsync(async (req, res, next) => {
  const customBannerId = req.params.id;

  if (req.file) {
    req.body.image = req.file.filename;
  }

  // Find the custom banner by id and update the details
  const customBanner = await CustomBanner.findByIdAndUpdate(
    customBannerId,
    req.body,
    {
      new: true,
      runValidators: true,
    }
  );

  // Check if the custom banner exists
  if (!customBanner) {
    return next(new AppError("Hero page not found", 404));
  }

  // Return the updated custom banner
  res.status(200).json({ customBanner });
});

// Function to delete a custom banner
export const deleteCustomBanner = catchAsync(async (req, res, next) => {
  const customBannerId = req.params.id;

  // Find the custom banner by id and delete it
  const customBanner = await CustomBanner.findByIdAndUpdate(
    customBannerId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the custom banner exists
  if (!customBanner) {
    return next(new AppError("Hero page not found", 404));
  }

  // Return the deleted custom banner
  res.status(204).json({ customBanner });
});
