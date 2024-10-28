import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import HeroPage from "../models/heroPage.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all hero pages
export const getAllHeroPages = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(HeroPage.find(), req.query)
    .filter()
    .sort()
    .limitFields();

  const heroPages = await features.query;

  // Return the hero pages
  res.status(200).json({ heroPages });
});

// Function to get a single hero page
export const getHeroPage = catchAsync(async (req, res, next) => {
  const heroPageId = req.params.id;

  // Find the hero page by id
  const heroPage = await HeroPage.findById(heroPageId);

  // Check if the hero page exists
  if (!heroPage) {
    return next(new AppError("Hero page not found", 404));
  }

  // Return the hero page
  res.status(200).json({ heroPage });
});

// Function to create a hero page
export const createHeroPage = catchAsync(async (req, res, next) => {
  req.body.image = req.file.filename;
  const heroPage = await HeroPage.create(req.body);
  // Return the created hero page
  res.status(201).json({ heroPage });
});

// Function to update a hero page
export const updateHeroPage = catchAsync(async (req, res, next) => {
  const heroPageId = req.params.id;

  if (req.file) {
    req.body.image = req.file.filename;
  }

  // Find the hero page by id and update the details
  const heroPage = await HeroPage.findByIdAndUpdate(heroPageId, req.body, {
    new: true,
    runValidators: true,
  });

  // Check if the hero page exists
  if (!heroPage) {
    return next(new AppError("Hero page not found", 404));
  }

  // Return the updated hero page
  res.status(200).json({ heroPage });
});

// Function to delete a hero page
export const deleteHeroPage = catchAsync(async (req, res, next) => {
  const heroPageId = req.params.id;

  // Find the hero page by id and delete it
  const heroPage = await HeroPage.findByIdAndUpdate(
    heroPageId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the hero page exists
  if (!heroPage) {
    return next(new AppError("Hero page not found", 404));
  }

  // Return the deleted hero page
  res.status(204).json({ heroPage });
});
