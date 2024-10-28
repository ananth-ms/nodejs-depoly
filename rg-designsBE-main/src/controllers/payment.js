import sendEmail from "../utils/email.js";
import fs from "fs";
import ejs from "ejs";
import path from "path";
import { fileURLToPath } from "url";

import Transaction from "../models/transaction.js";
import CustomOrder from "../models/customOrder.js";

// Get the directory name of the current module file
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Create PayPal Order
const checkout = async (req, res) => {
  try {
    const transation = await Transaction.create({
      lineItems: req.body.products,
      user: req.body.userId,
      amount: req.body.totalAmount,
      paypalId: req.body.paypalId,
      hdfcOrderId: req.body.hdfcOrderId,
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
    const templatePath = path.join(__dirname, "..", "templates", "index.html");
    const template = fs.readFileSync(templatePath, "utf-8");
    const emailData = {
      customerName: req.body.customerName,
      products: req.body.products,
      totalAmount: req.body.totalAmount,
      zipLinks: req.body.zipLinks,
    };
    // Render the template with the data
    const html = ejs.render(template, emailData);
    const mailOptions = {
      from: process.env.SMTP_USER,
      to: req.body.email,
      subject: "Order Summary",
      html: html,
    };
    await sendEmail(mailOptions);
    res.status(200).json(transation);
  } catch (error) {
    console.log(error);
    res.status(500).json({ error: error.message });
  }
};

export default checkout;
