require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const fetch = require('node-fetch');
const request = require("request");
const cron = require('node-cron');
const http = require('http');

const app = express();
app.set('port', process.env.PORT || 8082);
app.use(bodyParser.json({ limit: '10mb', extended: true }));

// Extend Date prototype for time manipulation
Date.prototype.subTime = function (h) {
    this.setHours(this.getHours() - h);
    this.setMinutes(0, 0, 0);
    return this;
};

Date.prototype.roundTime = function () {
    this.setMinutes(0, 0, 0);
    return this;
};

const fetchDataAndSendReport = async () => {
    const startTime = new Date().subTime(1).toISOString();
    const endTime = new Date().roundTime().toISOString();

    const queryData = {
        query: `SELECT route.keyword,
                SUM(CASE WHEN status.keyword = 'Delivered' THEN 1 ELSE 0 END) AS Delivered,
                SUM(CASE WHEN status.keyword IN ('Delivered', 'Rejected', 'Unroutable', 'Undeliverable') THEN 1 ELSE 0 END) AS DLR,
                COUNT(*) AS Total,
                (CAST(SUM(CASE WHEN status.keyword = 'Delivered' THEN 1 ELSE 0 END) / COUNT(*) AS int) * 100) AS PercentDlvrd,
                (CAST(SUM(CASE WHEN status.keyword IN ('Delivered', 'Undeliverable', 'Rejected', 'Unroutable') THEN 1 ELSE 0 END) / COUNT(*) AS int) * 100) AS PercentDLR
                FROM messages
                WHERE scheduledtime BETWEEN '${startTime}' AND '${endTime}'
                  AND status IN ('Sent', 'Delivered', 'Rejected', 'Undeliverable', 'Unroutable')
                  AND registereddelivery = 'true'
                  AND length(route.keyword) > 1
                  AND route.keyword <> 'hubtel-blacklist'
                GROUP BY route
                ORDER BY Total DESC`
    };

    console.log("Attempting connection at", new Date().toISOString());

    try {
        const esUrl = process.env.ES_SQL_ENDPOINT;

        if (!esUrl || !process.env.TEAMS_WEBHOOK_URL) {
            console.error("Missing required environment variables.");
            return;
        }

        request.post({ url: esUrl, body: queryData, json: true }, async (error, response, body) => {
            if (error) throw new Error(error);

            const dbResponse = body.datarows;
            if (!dbResponse || !Array.isArray(dbResponse)) {
                console.log('No data available.');
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
            const currentDate = new Date().toISOString().split('T')[0];

            const webhookPayload = {
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
            };

            await fetch(process.env.TEAMS_WEBHOOK_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(webhookPayload)
            });

            console.log("Report sent to Teams.");
        });
    } catch (err) {
        console.error('Error:', err);
    }
};

// Run hourly
cron.schedule('0 * * * *', fetchDataAndSendReport);

// Run once on startup
fetchDataAndSendReport();

// Basic HTTP server
const server = http.createServer((req, res) => {
    res.writeHead(200);
    res.end('Health check: OK');
}).listen(() => {
    console.log("Server listening on port", server.address().port);
});
