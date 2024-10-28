import mongoose, { Schema } from "mongoose";

// Define mongoose schema for transaction
const transactionSchema = new mongoose.Schema({
  lineItems: [
    {
      product: {
        type: Schema.Types.ObjectId,
        ref: "Product",
      },
      amount: Number,
      name: String,
    },
  ],
  user: {
    type: Schema.Types.ObjectId,
    ref: "User",
    required: true,
  },
  amount: {
    type: Number,
    required: true,
  },

  // Optional fields
  customerName: {
    type: String,
    default: null,
  },
  customerEmail: {
    type: String,
    default: null,
  },
  zipLinks: {
    type: [String],
    default: null,
  },
  date: {
    type: Date,
    default: null,
  },
  customOrder: {
    type: Schema.Types.ObjectId,
    ref: "CustomOrder",
    default: null,
  },
  paypalId: {
    type: String,
    default: null,
  },
  hdfcOrderId: {
    type: String,
    default: null,
  },
  couponDiscount: {
    type: Number,
    default: null,
  },
  couponCode: {
    type: String,
    default: null,
  },
  isPaid: {
    type: Boolean,
    default: false,
  },
  isDeleted: {
    type: Boolean,
    default: false,
  },
});

// Pre-find middleware to filter out deleted transactions
transactionSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create model for transaction
const Transaction = mongoose.model("Transaction", transactionSchema);

export default Transaction;
