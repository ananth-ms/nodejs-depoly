import mongoose from "mongoose";

// Define mongoose schema for product
const productSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
  },
  category: {
    type: String,
    enum: ["embroidery design", "print design"],
    required: true,
  },
  price: {
    type: Number,
    required: true,
  },
  slug: {
    type: String,
    required: true,
    unique: true,
  },

  // Optional fields
  priceUpdateType: {
    type: String,
    enum: ["escalation", "reduction", "nothing"],
    default: "nothing",
  },
  pricePercentage: {
    type: Number,
    default: 0,
  },
  tags: {
    type: [String],
    default: [],
  },
  originalPrice: {
    type: Number,
    default: null,
  },
  priceValidityDate: {
    type: Date,
    default: null,
  },
  image: {
    type: String,
    default: null,
  },
  zip: {
    type: String,
    default: null,
  },

  metaHeading: {
    type: String,
    trim: true,
  },
  metaContent: {
    type: String,
    trim: true,
  },
  isFreebie: {
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
  sizes: {
    type: String,
    default: null,
  },
  stitchCount: {
    type: String,
    default: null,
  },
  colors: {
    type: String,
    default: null,
  },
  formats: {
    type: String,
    default: null,
  },
  quantity: {
    type: String,
    default: null,
  },
  buyersCount: {
    type: Number,
    default: 0,
  },
  wishlistCount: {
    type: Number,
    default: 0,
  },
  cartCount: {
    type: Number,
    default: 0,
  },
  reviewsCount: {
    type: Number,
    default: 0,
  },
  ratingsAverage: {
    type: Number,
    default: 0,
  },
  isDeleted: {
    type: Boolean,
    default: false,
  },
});

// Pre-find middleware to filter out deleted products
productSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create model for product
const Product = mongoose.model("Product", productSchema);

export default Product;
