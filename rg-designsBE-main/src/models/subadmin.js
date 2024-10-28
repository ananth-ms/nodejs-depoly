import mongoose from "mongoose";
import bcrypt from "bcryptjs";

const subadminSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    trim: true,
  },
  email: {
    type: String,
    required: true,
    unique: true,
    trim: true,
    lowercase: true,
    match: /^\S+@\S+\.\S+$/,
  },
  password: {
    type: String,
    required: true,
    minlength: 6,
    select: false,
  },

  passwordChangedAt: Date,

  // Optional fields
  isActive: {
    type: Boolean,
    default: true,
  },
  accessTabs: {
    type: Array,
    default: [],
  },
  roleName: {
    type: String,
    default: "subadmin",
  },
  isDeleted: {
    type: Boolean,
    default: false,
    select: false,
  },
});

// Hash the password before saving
subadminSchema.pre("save", async function (next) {
  if (this.isModified("password")) {
    const salt = await bcrypt.genSalt(10);
    this.password = await bcrypt.hash(this.password, salt);
  }
  next();
});

// Add a method to update passwordChangedAt property
subadminSchema.pre("save", function (next) {
  if (this.isModified("password") && !this.isNew) {
    this.passwordChangedAt = new Date(Date.now() - 1000);
  }
  next();
});

// Query middleware
subadminSchema.pre(/^find/, function (next) {
  this.find({ isDeleted: { $ne: true } });
  next();
});

// Add a method to compare passwords
subadminSchema.methods.comparePassword = async function (password) {
  return bcrypt.compare(password, this.password);
};

// Add a method to check if the password was changed after the token was issued
subadminSchema.methods.changedPasswordAfter = function (JWTTimestamp) {
  if (this.passwordChangedAt) {
    const changedTimestamp = this.passwordChangedAt.getTime() / 1000;
    return JWTTimestamp < changedTimestamp;
  }
  return false;
};

// Create the subadmin model
const Subadmin = mongoose.model("Subadmin", subadminSchema);

export default Subadmin;
