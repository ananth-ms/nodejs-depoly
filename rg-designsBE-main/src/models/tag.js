import mongoose from "mongoose";

// Define mongoose schema for tags
const tagSchema = new mongoose.Schema({
  tagName: {
    type: String,
    default: null,
  },
  tagValue: {
    type: String,
    default: null,
  },

  //Optional fields
  date: {
    type: Date,
    default: new Date(),
  },
  isDeleted: {
    type: Boolean,
    default: false,
  },
});

// Pre-find middleware to filter out deleted tags
tagSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Create the tags model
const Tag = mongoose.model("Tag", tagSchema);

export default Tag;
