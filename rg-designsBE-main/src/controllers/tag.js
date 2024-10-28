import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import Tag from "../models/tag.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all tags
export const getAllTags = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(Tag.find(), req.query).filter().sort().limitFields();

  const tags = await features.query;

  // Return the tags
  res.status(200).json({ tags });
});

// Function to get a single tag
export const getTag = catchAsync(async (req, res, next) => {
  const tagId = req.params.id;

  // Find the tag by id
  const tag = await Tag.findById(tagId);

  // Check if the tag exists
  if (!tag) {
    return next(new AppError("Tag not found", 404));
  }

  // Return the tag
  res.status(200).json({ tag });
});

// Function to create a tag
export const createTag = catchAsync(async (req, res, next) => {
  const tag = await Tag.create(req.body);

  // Return the created tag
  res.status(201).json({ tag });
});

// Function to update a tag
export const updateTag = catchAsync(async (req, res, next) => {
  const tagId = req.params.id;

  // Find the tag by id and update the details
  const tag = await Tag.findByIdAndUpdate(tagId, req.body, {
    new: true,
    runValidators: true,
  });

  // Check if the tag exists
  if (!tag) {
    return next(new AppError("Tag not found", 404));
  }

  // Return the updated tag
  res.status(200).json({ tag });
});

// Function to delete a tag
export const deleteTag = catchAsync(async (req, res, next) => {
  const tagId = req.params.id;

  // Find the tag by id and delete it
  const tag = await Tag.findByIdAndUpdate(
    tagId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the tag exists
  if (!tag) {
    return next(new AppError("Tag not found", 404));
  }

  // Return the deleted tag
  res.status(204).json({ tag });
});
