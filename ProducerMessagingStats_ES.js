require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const fetch = require('node-fetch');
const request = require('request');
const cron = require('node-cron');
const http = require('http');

const app = express();
app.set('port', process.env.PORT || 8085);
app.use(bodyParser.json({ limit: '10mb', extended: true }));

// Add utility methods to Date prototype
Date.prototype.subTime = function (h) {
    this.setHours(this.getHours() - h);
    this.setMinutes(0, 0, 0);
    return this;
};

Date.prototype.roundTime = function () {
    this.setMinutes(0, 0, 0);
    return this;
};

const fetchAndSendReport = async () => {
    const startTime = new Date().subTime(1).toISOString();
    const endTime = new Date().roundTime().toISOString();

    const queryData = {
        query: `
            SELECT gateway.keyword, 
                   SUM(CASE WHEN status.keyword = 'Delivered' THEN 1 ELSE 0 END) AS Delivered, 
                   SUM(CASE WHEN status.keyword IN ('Delivered', 'Failed', 'Undeliverable') THEN 1 ELSE 0 END) AS DLR, 
                   COUNT(*) AS Total, 
                   (CAST(SUM(CASE WHEN status.keyword = 'Delivered' THEN 1 ELSE 0 END) / COUNT(*) AS int) * 100) AS PercentDlvrd, 
                   (CAST(SUM(CASE WHEN status.keyword IN ('Delivered', 'Undeliverable', 'Failed', 'EXPIRED') THEN 1 ELSE 0 END) / COUNT(*) AS int) * 100) AS PercentDLR 
            FROM producer-messages 
            WHERE createdAt BETWEEN '${startTime}' AND '${endTime}' 
              AND status IN ('Sent', 'Delivered', 'Failed', 'Undeliverable', 'Pending') 
              AND isTestSms = false 
            GROUP BY gateway 
            ORDER BY Total DESC`
    };

    console.log(`Fetching data at ${new Date().toISOString()}`);

    try {
        const esUrl = process.env.ES_SQL_ENDPOINT;
        const webhookUrl = process.env.TEAMS_WEBHOOK_URL;

        if (!esUrl || !webhookUrl) {
            console.error("Missing required environment variables.");
            return;
        }

        request.post({ url: esUrl, body: queryData, json: true }, async (error, response, body) => {
            if (error) {
                console.error("Elasticsearch Error:", error);
                return;
            }

            const dbResponse = body.datarows;
            if (!dbResponse || dbResponse.length === 0) {
                console.log("No data available.");
                return;
            }

            const addComma = new Intl.NumberFormat('en-US');

            const createRow = (item) => `
                <tr>
                    <td>${item[0]}</td>
                    <td>${addComma.format(item[1])}</td>
                    <td>${addComma.format(item[2])}</td>
                    <td>${addComma.format(item[3])}</td>
                    <td>${parseFloat(item[4]).toFixed(1)}%</td>
                    <td>${parseFloat(item[5]).toFixed(1)}%</td>
                </tr>
            `;

            const createTable = (rows) => `
                <table>
                    <tr>
                        <th>Gateway</th>
                        <th>Delivered</th>
                        <th>DLR</th>
                        <th>Total</th>
                        <th>%Dlvrd</th>
                        <th>%DLR</th>
                    </tr>
                    ${rows}
                </table>
            `;

            const records = dbResponse.map(createRow).join('');
            const table = createTable(records);

            const startTimeSms = new Date().subTime(1).roundTime().toTimeString().split(" ")[0];
            const endTimeSms = new Date().roundTime().toTimeString().split(" ")[0];
            const currentDate = new Date().toISOString().slice(0, 10);

            await fetch(webhookUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "themeColor": "0076D7",
                    "summary": "Hourly Report",
                    "sections": [
                        { "markdown": true },
                        {
                            "startGroup": true,
                            "text": `From <b>${startTimeSms}</b> to <b>${endTimeSms} on ${currentDate}</b><hr>
                                <table style='background-color: #6495ED !important; border: 1px solid black !important;'>
                                    <tr style='text-align:right;'>
                                        <th style='text-align:left !important'>Gateway</th>
                                        <th>Delivered</th>
                                        <th>DLR</th>
                                        <th>Total</th>
                                        <th>%Dlvrd</th>
                                        <th>%DLR</th>
                                    </tr>
                                    ${records}
                                </table>`
                        }
                    ]
                })
            }).catch(err => console.error("Webhook Error:", err));
        });

    } catch (err) {
        console.error("Unexpected Error:", err);
    }
};

// Schedule the report job to run at the top of every hour
cron.schedule('0 * * * *', fetchAndSendReport);

// Execute immediately at startup
fetchAndSendReport();

// HTTP server for health checks
const server = http.createServer((req, res) => {
    res.writeHead(200);
    res.end('Health check: OK');
}).listen(() => {
    console.log("Server listening on port", server.address().port);
});
