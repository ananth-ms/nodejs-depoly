import mongoose from "mongoose";

// Define mongoose schema for custom banner
const customBannerSchema = new mongoose.Schema({
  image: {
    type: String,
    required: true,
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

// Pre-find middleware to filter out deleted custom banners
customBannerSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create model for custom banner
const CustomBanner = mongoose.model("CustomBanner", customBannerSchema);

export default CustomBanner;
