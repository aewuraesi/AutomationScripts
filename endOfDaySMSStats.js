require('dotenv').config();
const cron = require('node-cron');
const express = require('express');
const bodyParser = require('body-parser');
const { IncomingWebhook } = require('ms-teams-webhook');
const pg = require('pg');
const http = require('http');

const app = express();
app.use(bodyParser.json({ limit: '10mb', extended: true }));

// PostgreSQL configuration from environment variables
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

// Calculate previous day's time boundaries
const date = new Date();
const dateObj = new Date(date.setDate(date.getDate() - 1));
const startOfDay = new Date(dateObj.setHours(0, 0, 0, 0)).toISOString().replace(/T/, ' ').replace(/\..+/, '');
const endOfDay = new Date(dateObj.setHours(23, 59, 59, 999)).toISOString().replace(/T/, ' ').replace(/\..+/, '');

console.log("Start of Day:", startOfDay);
console.log("End of Day:", endOfDay);

const addComma = new Intl.NumberFormat('en-US');

const createRow = (item) => `
  <tr style="background-color: #FFE4E1 !important; text-align:left;">
    <td style="text-align:left !important">${item.route}</td>
    <td>${addComma.format(item.Delivered)}</td>
    <td>${addComma.format(item.DLR)}</td>
    <td>${addComma.format(item.Total)}</td>
    <td>${parseFloat(item.PercentDlvrd)}%</td>
    <td>${parseFloat(item.PercentDLR)}%</td>
  </tr>
`;

const main = async () => {
  const queryData = `
    SELECT route,
      SUM(CASE WHEN status = 'Delivered' OR (status = 'Sent' AND registereddelivery = false) THEN 1 ELSE 0 END) AS "Delivered",
      SUM(CASE WHEN status IN ('Delivered', 'Sent', 'Rejected', 'Undeliverable') THEN 1 ELSE 0 END) AS "DLR",
      COUNT(*) AS "Total",
      ROUND((SUM(CASE WHEN status = 'Delivered' OR (status = 'Sent' AND registereddelivery = false) THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 1) AS "PercentDlvrd",
      ROUND((SUM(CASE WHEN status IN ('Delivered', 'Sent', 'Rejected', 'Undeliverable') THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 1) AS "PercentDLR"
    FROM messages
    WHERE scheduledtime BETWEEN '${startOfDay}' AND '${endOfDay}'
      AND status IN ('Sent', 'Delivered', 'Rejected', 'Expired', 'Deleted', 'Undeliverable')
    GROUP BY route
    ORDER BY "Total" DESC
  `;

  console.log("Attempting DB connection at:", new Date().toISOString());

  try {
    const { rows } = await query(queryData);

    if (!rows.length) {
      console.log("No data available for the previous day.");
      process.exit(0);
    }

    const records = rows.map(createRow).join('');

    // Initialize Teams webhooks from environment
    const webhookUrl = process.env.TEAMS_WEBHOOK_URL;
    const errorWebhookUrl = process.env.TEAMS_ERROR_WEBHOOK_URL;

    if (!webhookUrl || !errorWebhookUrl) {
      console.error("Webhook URLs are not set in environment variables.");
      process.exit(1);
    }

    const webhook = new IncomingWebhook(webhookUrl);
    const errorWebhook = new IncomingWebhook(errorWebhookUrl);

    // Send message to Teams
    await webhook.send({
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "0078D4",
      "summary": "End of Day Messaging Stats",
      "sections": [
        { "markdown": true },
        {
          "startGroup": true,
          "text": `
            From <b>${startOfDay}</b> to <b>${endOfDay}</b>
            <hr>
            <table style='border: 1px solid black !important;'>
              <tr style='background-color: #6495ED !important; text-align:left;'>
                <th style='text-align:left !important'>Gateway</th>
                <th>Delivered</th>
                <th>DLR</th>
                <th>Total</th>
                <th>%Dlvrd</th>
                <th>%DLR</th>
              </tr>
              ${records}
            </table>
          `,
        },
      ],
    });

    console.log("Message sent successfully to Teams.");
    process.exit();

  } catch (err) {
    console.error("Database or message error:", err);
    const errorWebhook = new IncomingWebhook(process.env.TEAMS_ERROR_WEBHOOK_URL);
    await errorWebhook.send({
      "@type": "MessageCard",
      "@context": "http://schema.org/extensions",
      "themeColor": "FF0000",
      "summary": "❌ Messaging Report Error",
      "sections": [{
        "text": `❌ Error occurred while sending end-of-day stats:\n\n${err}`
      }]
    });
    process.exit(1);
  }
};

// Schedule to run daily at 00:30 AM
cron.schedule('30 0 * * *', main);

// Run immediately on start
main();

// Lightweight HTTP server for health check
const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('Health check: OK');
}).listen(() => {
  console.log("Server listening on port", server.address().port);
});
