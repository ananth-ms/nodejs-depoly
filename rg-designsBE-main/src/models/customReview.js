import mongoose, { Schema } from "mongoose";

// Define mongoose schema for review
const customReviewSchema = new mongoose.Schema({
  customOrder: {
    type: Schema.Types.ObjectId,
    ref: "CustomOrder",
    required: true,
  },
  user: {
    type: Schema.Types.ObjectId,
    ref: "User",
    required: true,
  },
  rating: {
    type: Number,
    required: true,
  },
  review: {
    type: String,
    required: true,
  },

  //Optional fields
  category: {
    type: String,
    default: null,
  },
  date: {
    type: Date,
    default: new Date(),
  },
  isDeleted: {
    type: Boolean,
    default: false,
  },
});

// Pre-find middleware to filter out deleted reviews
customReviewSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create the review model
const CustomReview = mongoose.model("CustomReview", customReviewSchema);

export default CustomReview;
