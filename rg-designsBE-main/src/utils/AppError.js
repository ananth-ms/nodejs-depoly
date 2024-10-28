//App Error class to handle errors in the application
class AppError extends Error {
  statusCode;

  // Initialize the error object
  constructor(message, statusCode) {
    super(message);
    this.statusCode = statusCode;

    // Ensure the error object has the correct prototype chain
    Object.setPrototypeOf(this, AppError.prototype);

    Error.captureStackTrace(this, this.constructor);
  }
}

export default AppError;
