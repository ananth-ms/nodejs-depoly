import catchAsync from "../utils/catchAsync.js";
import Transaction from "../models/transaction.js";
import User from "../models/user.js";

function getDateRange(year, month, day = null) {
  let startDate, endDate;

  if (day) {
    startDate = new Date(year, month - 1, day, 0, 0, 0);
    endDate = new Date(year, month - 1, day, 23, 59, 59);
  } else {
    startDate = new Date(year, month - 1, 1);
    endDate = new Date(year, month, 0, 23, 59, 59);
  }

  return { startDate, endDate };
}

async function getTransactionCount(startDate, endDate) {
  return await Transaction.countDocuments({
    date: {
      $gte: startDate,
      $lte: endDate,
    },
  });
}

async function getTransactionRevenue(startDate, endDate) {
  const filter = {
    isPaid: true,
    date: {
      $gte: startDate,
      $lte: endDate,
    },
  };

  const transactions = await Transaction.find(filter);

  return transactions.reduce(
    (total, transaction) => total + (transaction.amount || 0),
    0
  );
}

async function getUserCount(startDate, endDate) {
  return await User.countDocuments({
    createdAt: {
      $gte: startDate,
      $lte: endDate,
    },
  });
}

// Function to get report
const getReport = catchAsync(async (req, res, next) => {
  const { year, month, day } = req.query;

  const { startDate, endDate } = getDateRange(year, month, day);

  const [transactionsCount, revenue, usersCreated] = await Promise.all([
    getTransactionCount(startDate, endDate),
    getTransactionRevenue(startDate, endDate),
    getUserCount(startDate, endDate),
  ]);

  const report = {
    transactionsCount,
    revenue,
    usersCreated,
  };

  // Return the report
  res.status(200).json({ report });
});

export default getReport;
