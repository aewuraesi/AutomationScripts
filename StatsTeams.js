//this scripts pulls data from a database and sends it to a teams channel
var express = require('express');
var app = express();
const bodyParser = require('body-parser');
const { IncomingWebhook } = require('ms-teams-webhook');
const request = require("request");
const pg = require('pg');

app.set('port', 8081);
app.use(bodyParser.json({ limit: '10mb', extended: true }));

const config = {
    user: 'user',
    host: 'host',
    database: 'database',
    password: 'password',
    port: 5432,
};
const pool = new pg.Pool(config);

async function query(q) {
    const client = await pool.connect()
    let res;
    try {
        await client.query('BEGIN')
        try {
            res = await client.query(q)
            await client.query('COMMIT')
        } catch (err) {
            await client.query('ROLLBACK')
            throw err
        }
    } finally {
        client.release()
    }
    return res
}

// Get Hourly Report 
// Rounding all times
//  subtracting 1 hour from current time as at when app was called
Date.prototype.subTime = function (h) {
    this.setHours(this.getHours() - h);
    this.setMinutes(0, 0, 0)
    return this;
};

var startTime = new Date().subTime(1).toISOString().replace(/T/, ' ').replace(/\..+/, '');

Date.prototype.roundTime = function () {
    this.setMinutes(0, 0, 0)
    return this;
};

var endTime = new Date().roundTime().toISOString().replace(/T/, ' ').replace(/\..+/, '');

console.log(startTime);
console.log(endTime);

var currentQueryDate = new Date(new Date().getFullYear(), new Date().getMonth(), new Date().getDate()).toLocaleDateString();

console.log(currentQueryDate);

addComma = new Intl.NumberFormat('en-US');

const createRow = (item) => `
    <tr>
      <td>${item.column1}</td>
    <td>${addComma.format(item.column2)}</td>
      <td>${addComma.format(item.column3)}</td>
      <td>${addComma.format(item.column4)}</td>
      <td>${parseFloat(item.column5)}%</td>
      <td>${parseFloat(item.column6)}%</td>
    </tr>
  `;

const createTable = (rows) => `
    <table>
      <tr>
          <th">column1 header</td>
          <th>column2 header</td>
          <th>column3 header</td>
          <th>column4 header</td>
          <th>%column5 header</td>
          <th>%column6 header</td>
      </tr>
      ${rows}
    </table>
  `;

const main = async (req, res) => {
    const queryData = `SELECT columns
      from tablename
      WHERE scheduledtime BETWEEN '${startTime}' AND '${endTime}'`;

    console.log("Attemptimg Connection at " + new Date().toISOString().replace(/T/, ' ').replace(/\..+/, ''));

    try {
        const { rows } = await query(queryData);
        const dbResponse = rows;

        if (dbResponse[0] === undefined) {
            console.log('There is no data');
            process.exit();
        }

        const records = dbResponse.map(createRow).join('');
        const table = createTable(records);

        // Webhook Initiation
        var webhookUrl = 'https://hubtel10.webhook.office.com/webhookb2/{webhook-id}';
        const webhook = new IncomingWebhook(webhookUrl);

        const sentFrom = "Sender ID";
        const client_id = "client-id";
        const client_secret = "client-secret";
        const contact_number = [
            {
                name: 'John',
                number: 12345
            },
 
            {
                name: 'Tom',
                number: 67890
            },
			{
                name: 'Frank',
                number: 54321
            },
			
        ];
        const client_reference = 1234;

        var startTimeSms = new Date().subTime(1).roundTime().toTimeString().split(" ")[0]

        var endTimeSms = new Date().roundTime().toTimeString().split(" ")[0]

        console.log(startTimeSms);
        console.log(endTimeSms);
        //console.log(dbResponse);
        for (let i = 0; i < dbResponse.length; i++) {
            var gatewayName = dbResponse[i].Gateway;


            for (let j = 0; j < contact_number.length; j++) {
                var smsUrl;
                var lowDlrmsg;

                if ((dbResponse[i].PercentDlvrd <= 80 || dbResponse[i].PercentDLR < 90)) {

                    lowDlrmsg = `Message details for between ${startTimeSms} AND ${endTimeSms}`;

                    smsUrl = `https://send.sms.url?From=${sentFrom}&To=${contact_number[j].number}&Content=${lowDlrmsg}&ClientReference=${client_reference}&clientid=${client_id}&clientsecret=${client_secret}&RegisteredDelivery=true`;

                    const sendSms = request.get(smsUrl, (error, response, body) => {
                        if (error) throw new Error(error);
                        console.log(`Alert Sms Sent To ${contact_number[j].name}`);
                    });
                }

                else {
                    console.log(`No Alert Sms Sent To ${contact_number[j].name}`);
                }
            }
        }

        (async () => {
            await webhook.send(JSON.stringify({
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "0076D7",
                "summary": "None",
                "sections": [
                    {
                        "markdown": true
                    },
                    {
                        "startGroup": true,
                        "text": `From <b>${startTime}</b> to <b>${endTime}</b> \n<hr>\n <table style='background-color: #6495ED !important; border: 1px solid black !important;'><tr style='text-align:right;'><th style='text-align:left !important'>Gateway</td><th>Delivered</td><th>DLR</td><th>Total</td><th>%Dlvrd</td><th>%DLR</td><tr><td>${records}</td></tr></table>`
                    }
                ]
            })
            ).then(function (res) {
                if (JSON.stringify(res.text).includes("Webhook message delivery failed with error: Microsoft Teams endpoint returned HTTP error 429 with ContextId")) {
                    var msg = `${JSON.stringify(res.text)}`;

                    var smsUrl = `https://send.sms.url?From=${sentFrom}&To=${contact_number}&Content=${msg}&ClientReference=${client_reference}&clientid=${client_id}&clientsecret=${client_secret}&RegisteredDelivery=true`;

                    request.get(smsUrl, (error, response, body) => {
                        if (error) throw new Error(error);
                        console.log("ALERT SMS SENT");
                        process.exit();
                    });
                };
            }).catch(function (err) {
                // handle request error
                var msg = `${err}`;
                console.log(msg)

                var smsUrl = `https://send.sms.url?From=${sentFrom}&To=${contact_number}&Content=${msg}&ClientReference=${client_reference}&clientid=${client_id}&clientsecret=${client_secret}&RegisteredDelivery=true`;
                
                request.get(smsUrl, (error, response, body) => {
                    if (error) throw new Error(error);
                    console.log("ERROR SMS SENT");
                    process.exit();
                });
            });
            process.exit();
        })();

    } catch (err) {
        console.log('Database ' + err);
        console.log("Response time after attempt: " + new Date().toISOString().replace(/T/, ' ').replace(/\..+/, ''));
        process.exit();
    }
}
main();


app.listen(app.get('port'), function () {
    console.log("Ewuresi's app is running on -->" + app.get('port'));

})