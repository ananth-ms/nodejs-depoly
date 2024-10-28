import { fileURLToPath } from "url";
import path from "path";
import fs from "fs";
import ejs from "ejs";
import Transaction from "../models/transaction.js";
import CustomOrder from "../models/customOrder.js";
import sendEmail from "../utils/email.js";
// Get the directory name of the current module file
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// block:start:importing-sdk
import { Juspay, APIError } from "expresscheckout-nodejs";
// block:end:importing-sdk

/**
 * Setup expresscheckout-node sdk
 */
const SANDBOX_BASE_URL = "https://smartgatewayuat.hdfcbank.com";
const PRODUCTION_BASE_URL = "https://smartgateway.hdfcbank.com";

/**
 * Read config.json file
 */
const configPath = path.join(__dirname, "../hdfc_config");
const configFilePath = path.join(configPath, "config.json");
const config = JSON.parse(fs.readFileSync(configFilePath, "utf-8"));

const publicKey = fs.readFileSync(path.join(configPath, config.PUBLIC_KEY_PATH));
const privateKey = fs.readFileSync(path.join(configPath, config.PRIVATE_KEY_PATH));
const paymentPageClientId = config.PAYMENT_PAGE_CLIENT_ID; // used in orderSession request

/*
Juspay.customLogger = Juspay.silentLogger
*/
const juspay = new Juspay({
  merchantId: config.MERCHANT_ID,
  baseUrl: SANDBOX_BASE_URL,
  jweAuth: {
    keyId: config.KEY_UUID,
    publicKey,
    privateKey,
  },
});

// Utitlity functions
function makeError(message) {
  return {
    message: message || "Something went wrong",
  };
}

function makeJuspayResponse(successRspFromJuspay) {
  if (successRspFromJuspay == undefined) return successRspFromJuspay;
  if (successRspFromJuspay.http != undefined) delete successRspFromJuspay.http;
  return successRspFromJuspay;
}
// block:start:session-function
export const initiateJuspayPayment = async (req, res) => {
  const orderId = `order_${Date.now()}`;
  const amount = req.body.totalAmount;
  // makes return url
  const returnUrl = `https://api.rgembroiderydesigns.com/api/payment/handleJuspayResponse`;
  // const returnUrl = `http://localhost:3000/api/payment/handleJuspayResponse`;

  try {
    const sessionResponse = await juspay.orderSession.create({
      order_id: orderId,
      amount: amount,
      payment_page_client_id: paymentPageClientId, // [required] shared with you, in config.json
      customer_id: req.body.userId, // [optional] your customer id here
      action: "paymentPage", // [optional] default is paymentPage
      return_url: returnUrl, // [optional] default is value given from dashboard
      currency: "INR", // [optional] default is INR
    });

    await Transaction.create({
      lineItems: req.body.products,
      user: req.body.userId,
      amount: req.body.totalAmount,
      hdfcOrderId: orderId,
      customOrder: req.body.customOrder || null,
      isPaid: req.body.isPaid,
      date: req.body.date,
      customerName: req.body.customerName,
      customerEmail: req.body.email,
      zipLinks: req.body.zipLinks,
    });
    if (req.body.customOrder && req.body.isPaid) {
      await CustomOrder.findByIdAndUpdate(req.body.customOrder, { isPaid: true });
    }

    // removes http field from response, typically you won't send entire structure as response
    return res.json(makeJuspayResponse(sessionResponse));
  } catch (error) {
    if (error instanceof APIError) {
      // handle errors comming from juspay's api
      return res.json(makeError(error.message));
    }
    return res.json(makeError());
  }
};
// block:end:session-function

// block:start:order-status-function
export const handleJuspayResponse = async (req, res) => {
  const orderId = req.body.order_id || req.body.orderId;

  if (orderId == undefined) {
    return res.json(makeError("order_id not present or cannot be empty"));
  }

  try {
    const statusResponse = await juspay.order.status(orderId);
    const orderStatus = statusResponse.status;
    let message = "";
    switch (orderStatus) {
      case "CHARGED":
        const transaction = await Transaction.findOneAndUpdate({ hdfcOrderId: orderId }, { isPaid: true });
        if (transaction.customOrder) {
          await CustomOrder.findByIdAndUpdate(transaction.customOrder, { isPaid: true });
        }
        const templatePath = path.join(__dirname, "..", "templates", "index.html");
        const template = fs.readFileSync(templatePath, "utf-8");
        const emailData = {
          customerName: transaction.customerName,
          products: transaction.lineItems,
          totalAmount: transaction.amount,
          zipLinks: transaction.zipLinks,
        };
        // Render the template with the data
        const html = ejs.render(template, emailData);
        const mailOptions = {
          from: process.env.SMTP_USER,
          to: transaction.email,
          subject: "Order Summary",
          html: html,
        };
        await sendEmail(mailOptions);
        message = "order payment done successfully";
        break;
      case "PENDING":
      case "PENDING_VBV":
        message = "order payment pending";
        break;
      case "AUTHORIZATION_FAILED":
        message = "order payment authorization failed";
        break;
      case "AUTHENTICATION_FAILED":
        message = "order payment authentication failed";
        break;
      default:
        message = "order status " + orderStatus;
        break;
    }

    // removes http field from response, typically you won't send entire structure as response
    res.redirect(`https://www.rgembroiderydesigns.com/order-success`);
  } catch (error) {
    if (error instanceof APIError) {
      // handle errors comming from juspay's api,
      return res.json(makeError(error.message));
    }
    return res.json(makeError());
  }
};
