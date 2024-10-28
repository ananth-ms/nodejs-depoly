import multer, { MulterError } from "multer";
import AppError from "./AppError.js";

import { v4 as uuidv4 } from "uuid";
import path from "path";
import { fileURLToPath } from "url";

// Get the directory name of the current module file
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Multer error handler
const throwMulterError = (err, next) => {
  // Handle Multer errors
  if (err instanceof MulterError) {
    return next(new AppError("Error Uploading file", 400));
  }
  // Handle other errors
  return next(new AppError(err.message, 400));
};

// Define the storage configuration for disk storage
const storageImage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, path.resolve(__dirname, "../../public/images"));
  },
  filename: (req, file, cb) => {
    const uniqueName = uuidv4() + path.extname(file.originalname);
    cb(null, uniqueName);
  },
});

// Define the file filter function for image validation
const fileFilterImage = (req, file, cb) => {
  if (file.mimetype.startsWith("image/")) {
    cb(null, true); // Accept the file
  } else {
    cb(new Error("Only image files are allowed!"), false);
  }
};

// Create the Multer middleware instance
const uploadImageMulter = multer({
  storage: storageImage,
  fileFilter: fileFilterImage,
});

// Define the route handler that uses the Multer middleware
const uploadImage = (req, res, next) => {
  uploadImageMulter.single("image")(req, res, (err) => {
    if (err) throwMulterError(err, next);
    if (!req.file) {
      if (req.method === "PATCH") return next();
      return next(new AppError("Please upload an image", 400));
    }

    // Create a unique filename
    req.file.originalname = uuidv4();
    next();
  });
};

// New Multer middleware instance for handling both image and zip
const uploadImageAndZipMulter = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => {
      console.log(file);
      if (file.mimetype.startsWith("image/")) {
        cb(null, path.resolve(__dirname, "../../public/images"));
      } else if (file.mimetype.includes("zip") || file.mimetype.includes("compressed")) {
        cb(null, path.resolve(__dirname, "../../public/zips"));
      }
    },
    filename: (req, file, cb) => {
      const uniqueName = uuidv4() + path.extname(file.originalname);
      cb(null, uniqueName);
    },
  }),
});

// New function to handle image and zip upload
const uploadImageAndZip = (req, res, next) => {
  uploadImageAndZipMulter.fields([
    { name: "image", maxCount: 1 },
    { name: "zip", maxCount: 1 },
  ])(req, res, (err) => {
    if (err) throwMulterError(err, next);
    if (!req.files || !req.files.image || !req.files.zip) {
      if (req.method === "PATCH") return next();
      return next(new AppError("Please upload both an image and a zip file", 400));
    }

    // Create unique filenames
    if (req.files.image) {
      req.files.image[0].originalname = uuidv4();
    }
    if (req.files.zip) {
      req.files.zip[0].originalname = uuidv4();
    }

    next();
  });
};

const uploadImageAndZipOptional = (req, res, next) => {
  uploadImageAndZipMulter.fields([
    { name: "image", maxCount: 1 },
    { name: "zip", maxCount: 1 },
  ])(req, res, (err) => {
    if (err) throwMulterError(err, next);
    if (req.files && req.files.image) {
      req.files.image[0].originalname = uuidv4();
    }
    if (req.files && req.files.zip) {
      req.files.zip[0].originalname = uuidv4();
    }
    next();
  });
};

export { uploadImage, uploadImageAndZip, uploadImageAndZipOptional };
