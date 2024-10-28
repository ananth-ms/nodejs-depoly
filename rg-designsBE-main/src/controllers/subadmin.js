import jwt from "jsonwebtoken";
import Subadmin from "../models/subadmin.js";
import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Cookie options
const cookieExpiresIn = parseInt(process.env.COOKIE_EXPIRES_IN);
const cookieOptions = {
  expires: new Date(Date.now() + cookieExpiresIn * 24 * 60 * 60 * 1000),
  httpOnly: false,
  secure: false,
  sameSite: "none",
};

// Function to sign the JWT token
const jwtSecret = process.env.JWT_SECRET;
const jwtExpiresIn = process.env.JWT_EXPIRES_IN;
const signToken = (subadminId) => {
  return jwt.sign({ subadminId }, jwtSecret, { expiresIn: jwtExpiresIn });
};

// Function to handle subadmin login
export const login = catchAsync(async (req, res, next) => {
  const { email, password } = req.body;

  // Check if the subadmin exists
  const subadmin = await Subadmin.findOne({ email }).select("+password");
  if (!subadmin) {
    return next(new AppError("Subadmin not found", 404));
  }

  // Check if the password is correct
  const isPasswordValid = await subadmin.comparePassword(password);
  if (!isPasswordValid) {
    return next(new AppError("Invalid password", 401));
  }

  // Generate JWT token
  const token = signToken(subadmin._id);

  // Set the token as a cookie
  res.cookie("token", token, cookieOptions);

  // Return success response
  res
    .status(200)
    .json({ message: "Login successful", token, role: subadmin.role });
});

// Function to handle subadmin createSubadmin
export const createSubadmin = catchAsync(async (req, res, next) => {
  // Check if the subadmin already exists
  const existingSubadmin = await Subadmin.findOne({ email: req.body.email });
  if (existingSubadmin) {
    return next(new AppError("Subadmin already exists", 400));
  }

  // Create a new subadmin
  const newSubadmin = new Subadmin(req.body);
  await newSubadmin.save();

  // Return success response
  res.status(201).json({ message: "Signup successful" });
});

// Function to handle protected route
export const subadminProtect = catchAsync(async (req, res, next) => {
  // Check if the subadmin is logged in
  let token;
  if (
    req.headers.authorization &&
    req.headers.authorization.startsWith("Bearer")
  ) {
    token = req.headers.authorization.split(" ")[1];
  } else if (req.cookies.token) {
    token = req.cookies.token;
  }
  if (!token) {
    return next(new AppError("You are not logged in", 401));
  }

  // Verify the JWT token
  const decodedToken = jwt.verify(token, jwtSecret);
  const subadminId = decodedToken.subadminId;

  // Check if the subadmin exists
  const subadmin = await Subadmin.findById(subadminId);
  if (!subadmin) {
    return next(new AppError("Subadmin not found", 404));
  }

  // Check if the subadmin changed the password after the token was issued
  if (subadmin.changedPasswordAfter(decodedToken.iat)) {
    return next(new AppError("Subadmin recently changed password", 401));
  }

  // Attach the subadmin object to the request
  req.subadmin = subadmin;

  // Continue to the next middleware
  next();
});

// Function to get the current subadmin
export const getCurrentSubadmin = catchAsync(async (req, res, next) => {
  // Return the subadmin object
  res.status(200).json({ subadmin: req.subadmin });
});

// Function to handle subadmin logout
export const logout = (req, res) => {
  // Clear the token cookie
  res.clearCookie("token");

  // Return success response
  res.status(200).json({ message: "Logout successful" });
};

// Function to update the password
export const updatePassword = catchAsync(async (req, res, next) => {
  const { currentPassword, newPassword } = req.body;

  // Check if the current password is correct
  const subadmin = await Subadmin.findById(req.params.id).select("+password");
  if (!subadmin) {
    return next(new AppError("Subadmin not found", 404));
  }

  const isPasswordValid = await subadmin.comparePassword(currentPassword);
  if (!isPasswordValid) {
    return next(new AppError("Invalid password", 401));
  }

  // Update the password
  subadmin.password = newPassword;
  await subadmin.save();

  // Return success response
  res.status(200).json({ message: "Password update successful" });
});

// Function to get all subadmins
export const getAllSubadmins = catchAsync(async (req, res, next) => {
  // Fetch all subadmins from the database
  const features = new APIFeatures(Subadmin.find(), req.query) // Cast req.query to QueryString type

    .filter()
    .sort()
    .limitFields();
  // .paginate();

  const subadmins = await features.query;

  // const totalDocuments = await new APIFeatures(
  //   Subadmin.find(),
  //   req.query
  // ).countDocuments();

  // const page = req.query.page ? parseInt(req.query.page) : 1;
  // const limit = req.query.limit ? parseInt(req.query.limit) : 10;
  // const totalPages = Math.ceil(totalDocuments / limit);

  // const response = {
  //   page,
  //   perPage: limit,
  //   totalPages,
  //   totalResults: totalDocuments,
  //   results: subadmins.length,
  //   subadmins,
  // };

  // Return the subadmins
  res.status(200).json({ subadmins });
});

// Function to get a single subadmin
export const getSubadmin = catchAsync(async (req, res, next) => {
  const subadminId = req.params.id;

  // Find the subadmin by id
  const subadmin = await Subadmin.findById(subadminId);

  // Check if the subadmin exists
  if (!subadmin) {
    return next(new AppError("Subadmin not found", 404));
  }

  // Return the subadmin
  res.status(200).json({ subadmin });
});

// Function to update a subadmin
export const updateSubadmin = catchAsync(async (req, res, next) => {
  // Check if the subadmin is trying to update the password
  if (req.body.password) {
    return next(new AppError("This route is not for password updates", 400));
  }

  const subadminId = req.params.id;

  // Find the subadmin by id and update the details
  const subadmin = await Subadmin.findByIdAndUpdate(subadminId, req.body, {
    new: true,
  });

  // Check if the subadmin exists
  if (!subadmin) {
    return next(new AppError("Subadmin not found", 404));
  }

  // Return the updated subadmin
  res.status(200).json({ subadmin });
});

// Function to delete a subadmin
export const deleteSubadmin = catchAsync(async (req, res, next) => {
  const subadminId = req.params.id;

  // Find the subadmin by id and delete it
  const subadmin = await Subadmin.findByIdAndUpdate(
    subadminId,
    { isDeleted: true },
    { new: true }
  );

  // Check if the subadmin exists
  if (!subadmin) {
    return next(new AppError("Subadmin not found", 404));
  }

  // Return the deleted subadmin
  res.status(204).json({ subadmin });
});
