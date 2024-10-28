import catchAsync from "../utils/catchAsync.js";
import AppError from "../utils/AppError.js";
import Product from "../models/product.js";
import APIFeatures from "../utils/ApiFeatures.js";

// Function to get all products
export const getAllProducts = catchAsync(async (req, res, next) => {
  const features = new APIFeatures(Product.find(), req.query).filter().sort().limitFields();

  const products = await features.query;

  // Return the products
  res.status(200).json({ products });
});

// Function to get a single product
export const getProduct = catchAsync(async (req, res, next) => {
  const productId = req.params.id;

  // Find the product by id
  const product = await Product.findById(productId);

  // Check if the product exists
  if (!product) {
    return next(new AppError("Product not found", 404));
  }

  // Return the product
  res.status(200).json({ product });
});
export const getProductBySlug = catchAsync(async (req, res, next) => {
  const slug = req.params.slug;

  // Find the product by slug
  const product = await Product.findOne({ slug });

  // Check if the product exists
  if (!product) {
    return next(new AppError("Product not found", 404));
  }

  // Return the product
  res.status(200).json({ product });
});
// Function to create a product
export const createProduct = catchAsync(async (req, res, next) => {
  if (req.files.image) {
    req.body.image = req.files.image[0].filename;
  }
  if (req.files.zip) {
    req.body.zip = req.files.zip[0].filename;
  }

  const product = await Product.create(req.body);
  // Return the created product
  res.status(201).json({ product });
});

// Function to update a product
export const updateProduct = catchAsync(async (req, res, next) => {
  const productId = req.params.id;

  if (req.files) {
    if (req.files.image) {
      req.body.image = req.files.image[0].filename;
    }
    if (req.files.zip) {
      req.body.zip = req.files.zip[0].filename;
    }
  }

  // Find the product by id and update the details
  const product = await Product.findByIdAndUpdate(productId, req.body, {
    new: true,
    runValidators: true,
  });

  // Check if the product exists
  if (!product) {
    return next(new AppError("Product not found", 404));
  }

  // Return the updated product
  res.status(200).json({ product });
});

// Function to delete a product
export const deleteProduct = catchAsync(async (req, res, next) => {
  const productId = req.params.id;

  // Find the product by id and delete it
  const product = await Product.findByIdAndUpdate(
    productId,
    {
      isDeleted: true,
    },
    {
      new: true,
    }
  );

  // Check if the product exists
  if (!product) {
    return next(new AppError("Product not found", 404));
  }

  // Return the deleted product
  res.status(204).json({ product });
});

export const bulkUpdate = catchAsync(async (req, res, next) => {
  const { ids } = req.body;

  if (!ids) {
    return next(new AppError("Please provide ids", 400));
  }

  // Fetch the products by ids
  const products = await Product.find({ _id: { $in: ids } });

  // Prepare an array of update operations
  const bulkOperations = products.map((product) => {
    const newOriginalPrice = product.price; // Assuming you're setting originalPrice to the current price

    return {
      updateOne: {
        filter: { _id: product._id },
        update: {
          $set: {
            ...req.body, // Keep the existing fields from the request body
            originalPrice: newOriginalPrice, // Set the originalPrice to the product's current price
          },
        },
      },
    };
  });

  // Perform bulkWrite to update all products in one go
  await Product.bulkWrite(bulkOperations);

  res.status(200).json({ message: "Products updated successfully" });
});

export const bulkUpdateAll = catchAsync(async (req, res, next) => {
  // Fetch all products
  const products = await Product.find({});

  // Prepare an array of update operations
  const bulkOperations = products.map((product) => {
    const newOriginalPrice = product.price; // Assuming you're setting originalPrice to the current price

    return {
      updateOne: {
        filter: { _id: product._id },
        update: {
          $set: {
            ...req.body, // Keep the existing fields from the request body
            originalPrice: newOriginalPrice, // Set the originalPrice to the product's current price
          },
        },
      },
    };
  });

  // Perform bulkWrite to update all products in one go
  await Product.bulkWrite(bulkOperations);

  res.status(200).json({ message: "All products updated successfully" });
});

// Controller function for product search by name
export const searchProduct = catchAsync(async (req, res, next) => {
  const { name } = req.query;

  if (!name) {
    return next(new AppError("Please provide a name", 400));
  }

  // Perform case-insensitive search for products containing the name
  const products = await Product.find({
    name: { $regex: new RegExp(name, "i") },
  });

  res.status(200).json({ products });
});
