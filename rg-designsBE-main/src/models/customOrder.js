import mongoose from "mongoose";

// Define mongoose schema for custom order
const customOrderSchema = new mongoose.Schema({
  category: {
    type: String,
    enum: ["custom digitizing", "vector tracing"],
    required: true,
  },
  user: {
    type: mongoose.Schema.ObjectId,
    ref: "User",
    required: true,
  },

  // Optional fields
  image: {
    type: String,
    default: null,
  },
  zip: {
    type: String,
    default: null,
  },
  quoteAmount: {
    type: Number,
    default: null,
  },
  isPaid: {
    type: Boolean,
    default: false,
  },
  date: {
    type: Date,
    default: new Date(),
  },
  description: {
    type: String,
    default: null,
  },
  size: {
    type: String,
    default: null,
  },
  fileFormat: {
    type: String,
    default: null,
  },
  isDeleted: {
    type: Boolean,
    default: false,
  },
});

// Pre-find middleware to filter out deleted products
customOrderSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create model for custom order
const CustomOrder = mongoose.model("CustomOrder", customOrderSchema);

export default CustomOrder;
