import nodemailer from "nodemailer";

// Create a transporter
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST,
  port: parseInt(process.env.SMTP_PORT),
  secure: process.env.SMTP_SECURE === "true" || false,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
});

// Define a function to send an email
const sendEmail = async (mailOptions) => {
  try {
    if (process.env.SEND_EMAIL === "false") {
      console.log("Email not sent because SEND_EMAIL is set to false");
      return;
    }
    const info = await transporter.sendMail(mailOptions);
    return info;
  } catch (error) {
    throw error;
  }
};

export default sendEmail;
