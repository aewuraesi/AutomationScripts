require('dotenv').config();
const pg = require('pg');
const nodemailer = require('nodemailer');
const cron = require('node-cron');
const http = require('http');

// PostgreSQL configuration via environment variables
const config = {
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
};

const pool = new pg.Pool(config);

async function query(q) {
  const client = await pool.connect();
  let res;
  try {
    await client.query('BEGIN');
    try {
      res = await client.query(q);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    }
  } finally {
    client.release();
  }
  return res;
}

// Helper methods for date rounding
Date.prototype.subTime = function (h) {
  this.setHours(this.getHours() - h);
  this.setMinutes(0, 0, 0);
  return this;
};
Date.prototype.roundTime = function () {
  this.setMinutes(0, 0, 0);
  return this;
};

async function sendEmail(htmlText) {
  const mail = nodemailer.createTransport({
    host: process.env.SES_SMTP_HOST,
    port: process.env.SES_SMTP_PORT,
    secure: false,
    auth: {
      user: process.env.SES_SMTP_USER,
      pass: process.env.SES_SMTP_PASS,
    },
  });

  const recipients = process.env.REPORT_EMAILS?.split(',') || [];

  const mailOptions = {
    from: process.env.REPORT_EMAIL_SENDER || 'no-reply@example.com',
    to: recipients.join(','),
    subject: 'Bulk SMS Hourly Stats',
    html: htmlText,
  };

  mail.sendMail(mailOptions, function (error, info) {
    if (error) {
      console.error('Email error:', error);
    } else {
      console.log('Email sent:', info.response);
    }
  });
}

const addComma = new Intl.NumberFormat('en-US');

const createRow = (item, currentQueryDate) => `
  <tr style="background-color: #FFE4E1 !important; text-align:right;">
    <td style="text-align:left !important">${currentQueryDate}</td>
    <td style="text-align:left !important">${item.Gateway}</td>
    <td>${addComma.format(item.Delivered)}</td>
    <td>${addComma.format(item.DLR)}</td>
    <td>${addComma.format(item.Total)}</td>
    <td>${parseFloat(item.PercentDlvrd)}%</td>
    <td>${parseFloat(item.PercentDLR)}%</td>
  </tr>
`;

const createTable = (rows) => `
  <table style="border: 1px solid black !important;">
    <tr style="background-color: #6495ED !important; text-align:right;">
        <th style="text-align:left !important">QTime</th>
        <th style="text-align:left !important">Gateway</th>
        <th>Delivered</th>
        <th>DLR</th>
        <th>Total</th>
        <th>%Dlvrd</th>
        <th>%DLR</th>
    </tr>
    ${rows}
  </table>
`;

const createHtml = (table, startTime, endTime) => `
  <html>
    <head>
      <style>
        table { width: 100%; }
        tr { border: 1px solid black !important; }
        th, td { padding: 10px; }
        tr:nth-child(odd) { background-color: #C0C0C0 !important; }
        tr:nth-child(even) { background-color: #FFE4E1 !important; }
        .no-content { background-color: #F08080 !important; }
      </style>
    </head>
    <body>
      <p>Hello All,</p>
      <p>Please see below SMS Hourly Statistics Report. Report is for the previous hour, from ${startTime} to ${endTime}.</p>
      <br>
      ${table}
    </body>
  </html>
`;

const main = async () => {
  const startTime = new Date().subTime(1).toISOString().split('.')[0].replace('T', ' ');
  const endTime = new Date().roundTime().toISOString().split('.')[0].replace('T', ' ');
  const currentQueryDate = new Date().toLocaleDateString();

  const queryData = `
    SELECT route AS "Gateway",
      SUM(CASE WHEN status = 'Delivered' OR (status = 'Sent' AND registereddelivery = false) THEN 1 ELSE 0 END) AS "Delivered",
      SUM(CASE WHEN status IN ('Delivered', 'Sent', 'Rejected', 'Undeliverable') THEN 1 ELSE 0 END) AS "DLR",
      COUNT(*) AS "Total",
      ROUND((SUM(CASE WHEN status = 'Delivered' OR (status = 'Sent' AND registereddelivery = false) THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 1) AS "PercentDlvrd",
      ROUND((SUM(CASE WHEN status IN ('Delivered', 'Sent', 'Rejected', 'Undeliverable') THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 1) AS "PercentDLR"
    FROM messages
    WHERE scheduledtime BETWEEN '${startTime}' AND '${endTime}'
      AND status IN ('Sent', 'Delivered', 'Rejected', 'Expired', 'Deleted', 'Undeliverable')
      AND registereddelivery = true
    GROUP BY route
    ORDER BY "Total" DESC
  `;

  console.log("Attempting DB connection at:", new Date().toISOString());

  try {
    const { rows } = await query(queryData);

    if (!rows.length) {
      console.log('No data returned for the previous hour.');
      process.exit(0);
    }

    const tableRows = rows.map(item => createRow(item, currentQueryDate)).join('');
    const table = createTable(tableRows);
    const html = createHtml(table, startTime, endTime);
    await sendEmail(html);

  } catch (err) {
    console.error('Database error:', err);
    process.exit(1);
  }
};

// Schedule: run at the top of every hour
cron.schedule('0 * * * *', main);

// Run on startup
main();

// Lightweight HTTP server (for health checks, etc.)
const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('Health check: OK');
}).listen(() => {
  console.log("Server listening on port", server.address().port);
});
