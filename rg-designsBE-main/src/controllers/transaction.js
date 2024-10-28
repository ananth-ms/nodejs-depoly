import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import Transaction from "../models/transaction.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all transactions
export const getAllTransactions = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(Transaction.find(), req.query)
    .filter()
    .sort()
    .limitFields();

  const transactions = await features.query;

  // Return the transactions
  res.status(200).json({ transactions });
});

// Function to get a single transaction
export const getTransaction = catchAsync(async (req, res, next) => {
  const transactionId = req.params.id;

  // Find the transaction by id
  const transaction = await Transaction.findById(transactionId);

  // Check if the transaction exists
  if (!transaction) {
    return next(new AppError("Transaction not found", 404));
  }

  // Return the transaction
  res.status(200).json({ transaction });
});

// Function to create a transaction
export const createTransaction = catchAsync(async (req, res, next) => {
  const transaction = await Transaction.create(req.body);
  // Return the created transaction
  res.status(201).json({ transaction });
});

// Function to update a transaction
export const updateTransaction = catchAsync(async (req, res, next) => {
  const transactionId = req.params.id;

  // Find the transaction by id and update the details
  const transaction = await Transaction.findByIdAndUpdate(
    transactionId,
    req.body,
    {
      new: true,
      runValidators: true,
    }
  );

  // Check if the transaction exists
  if (!transaction) {
    return next(new AppError("Transaction not found", 404));
  }

  // Return the updated transaction
  res.status(200).json({ transaction });
});

// Function to delete a transaction
export const deleteTransaction = catchAsync(async (req, res, next) => {
  const transactionId = req.params.id;

  // Find the transaction by id and delete it
  const transaction = await Transaction.findByIdAndDelete(transactionId);

  // Check if the transaction exists
  if (!transaction) {
    return next(new AppError("Transaction not found", 404));
  }

  // Return a success message
  res.status(204).json();
});
