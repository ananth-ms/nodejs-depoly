import mongoose from "mongoose";

// Define mongoose schema for freebieBanner
const freebieBannerSchema = new mongoose.Schema({
  image: {
    type: String,
    required: true,
  },

  // Optional fields
  isDeleted: {
    type: Boolean,
    default: false,
  },
});

// Pre-find middleware to filter out deleted freebieBanner
freebieBannerSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create model for freebieBanner
const FreebieBanner = mongoose.model("FreebieBanner", freebieBannerSchema);

export default FreebieBanner;
