import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import CustomOrder from "../models/customOrder.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all customOrders
export const getAllCustomOrders = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(CustomOrder.find(), req.query)
    .filter()
    .sort()
    .limitFields();

  const customOrders = await features.query;

  // Return the customOrders
  res.status(200).json({ customOrders });
});

// Function to get a single customOrder
export const getCustomOrder = catchAsync(async (req, res, next) => {
  const customOrderId = req.params.id;

  // Find the customOrder by id
  const customOrder = await CustomOrder.findById(customOrderId);

  // Check if the customOrder exists
  if (!customOrder) {
    return next(new AppError("CustomOrder not found", 404));
  }

  // Return the customOrder
  res.status(200).json({ customOrder });
});

// Function to create a customOrder
export const createCustomOrder = catchAsync(async (req, res, next) => {
  if (req.files.image) {
    req.body.image = req.files.image[0].filename;
  }
  if (req.files.zip) {
    req.body.zip = req.files.zip[0].filename;
  }

  req.body.user = req.user._id;
  const customOrder = await CustomOrder.create(req.body);
  // Return the created customOrder
  res.status(201).json({ customOrder });
});

// Function to update a customOrder
export const updateCustomOrder = catchAsync(async (req, res, next) => {
  const customOrderId = req.params.id;

  if (req.files) {
    if (req.files.image) {
      req.body.image = req.files.image[0].filename;
    }
    if (req.files.zip) {
      req.body.zip = req.files.zip[0].filename;
    }
  }
  // Find the customOrder by id and update the details
  const customOrder = await CustomOrder.findByIdAndUpdate(
    customOrderId,
    req.body,
    {
      new: true,
      runValidators: true,
    }
  );

  // Check if the customOrder exists
  if (!customOrder) {
    return next(new AppError("CustomOrder not found", 404));
  }

  // Return the updated customOrder
  res.status(200).json({ customOrder });
});

// Function to delete a customOrder
export const deleteCustomOrder = catchAsync(async (req, res, next) => {
  const customOrderId = req.params.id;

  // Find the customOrder by id and delete it
  const customOrder = await CustomOrder.findByIdAndUpdate(
    customOrderId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the customOrder exists
  if (!customOrder) {
    return next(new AppError("CustomOrder not found", 404));
  }

  // Return the deleted customOrder
  res.status(204).json({ customOrder });
});
