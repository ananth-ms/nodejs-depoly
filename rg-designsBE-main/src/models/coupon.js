import mongoose from "mongoose";

// Define mongoose schema for coupon
const couponSchema = new mongoose.Schema({
  image: {
    type: String,
    required: true,
  },
  couponCode: {
    type: String,
    required: true,
  },
  discountPercentage: {
    type: Number,
    required: true,
    min: 0,
    max: 100,
  },
  expiryDate: {
    type: Date,
    required: true,
  },

  // Optional fields
  isDeleted: {
    type: Boolean,
    default: false,
  },
});

// Pre-find middleware to filter out deleted coupon
couponSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create model for coupon
const Coupon = mongoose.model("Coupon", couponSchema);

export default Coupon;
