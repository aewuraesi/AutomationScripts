var express = require('express');
const bodyParser = require('body-parser');
var throttledQueue = require('throttled-queue');
const request = require("requestretry");
const pg = require('pg');
var app = express();
let http = require('http');


app.use(bodyParser.json({
    limit: '10mb',
    extended: true
}));

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

var startTimeOnly = new Date().subTime(1).toLocaleTimeString();
var endTimeOnly = new Date().roundTime().toLocaleTimeString();
console.log (startTimeOnly + " " + endTimeOnly);
var currentQueryDate = new Date(new Date().getFullYear(), new Date().getMonth(), new Date().getDate()).toLocaleDateString();
//console.log(currentQueryDate)

//throttledQueue(number of requests, per duration in seconds);
var throttle = throttledQueue(20, 1000);
const sentFrom = "fromAddreess";
const client_id = "clientid";
const client_secret = "clientsecret";
const client_reference = 1234;
const contact_number = [
    {
        name: 'Recipeint1',
        number: 1234
    },
	{
		name: 'Recipeint2',
		number: 5678
	}
]
// Get End of Day DLR data 
const main = async (req, res) => {
    const queryData = `SELECT column names
	FROM tablename
	where conditions
	and timeframe BETWEEN '${startTime}' AND '${endTime}'
	order by timeColumn ASC;`;
	
	currentTime = new Date().toISOString().replace(/T/, ' ').replace(/\..+/, '')

    console.log("Attemptimg Connection at " + currentTime);
    try {
        const {
            rows
        } = await query(queryData);
        const dbResponse = rows;

        if (dbResponse[0] === undefined) {
			for (let j = 0; j < contact_number.length; j++) {
				noDLR = `DLR processing data count is 0 for between ${startTimeOnly} and ${endTimeOnly}.`;

				smsUrl = `https://endpoint.to.use.for.sms?From=${sentFrom}&To=${contact_number[j].number}&Content=${noDLR}&ClientReference=${client_reference}&clientid=${client_id}&clientsecret=${client_secret}&RegisteredDelivery=true`;

				request.get(smsUrl, (error, response, body) => {
					if (error) throw new Error(error);
					console.log(`Alert Sms Sent To ${contact_number[j].name}`);
				});
			}
			console.log('There is no data');
            process.exit();
        }
		for (let j = 0; j < contact_number.length; j++) {
            startDLR = `DLR processing data count is ${dbResponse.length} for between ${startTimeOnly} and ${endTimeOnly}. Processing start time is ${new Date().toLocaleTimeString()}`;

            smsUrl = `https://endpoint.to.use.for.sms?From=${sentFrom}&To=${contact_number[j].number}&Content=${noDLR}&ClientReference=${client_reference}&clientid=${client_id}&clientsecret=${client_secret}&RegisteredDelivery=true`;

            request.get(smsUrl, (error, response, body) => {
                if (error) throw new Error(error);
                console.log(`Alert Sms Sent To ${contact_number[j].name}`);
			});
		}

         await dbResponse.forEach((data, index) => {
			 //console.log(data);
               var replacementId = data.id;
                var newId = replacementId.split("-").join("");

                var replacementrecipient = data.recipient;
                var newrecipient = replacementrecipient.split("+").join("");
				
				var clientref = data.clientreference;
                var msgstat = data.status;
                throttle(function () {
                   
                    //new endpoint
                    var dlvryReceiptUrl = `https://dlr.endpoint.to.use?MessageId=${newId}&ClientReference=${clientref}&Status=${msgstat}&From=${data.sender}&To=%2B${newrecipient}`
                    
                     request.get({
                        url: dlvryReceiptUrl,
                        json: true,
                        maxAttempts: 15, // (default) try 5 times
                        retryDelay: 5000, // (default) wait for 3s before trying again
                        retryStrategy: request.RetryStrategies.HTTPOrNetworkError // (default) retry on 5xx or network error
                    }, (error, response, body) => {
                         
                        if (response) {
							console.log(`${index}.ID:${newId}. ClientReference:${clientref}. Staus:${msgstat} : ${response.statusCode}.  Attempts: ${response.attempts}`);
							if (index === dbResponse.length-1 || index <! dbResponse.length-1 ){
								endDLR = `DLR processing for between ${startTimeOnly} and ${endTimeOnly} has ended. End time is ${new Date().toLocaleTimeString()}`;

								for (let j = 0; j < contact_number.length; j++) {
                                    smsUrl = `https://endpoint.to.use.for.sms?From=${sentFrom}&To=${contact_number[j].number}&Content=${noDLR}&ClientReference=${client_reference}&clientid=${client_id}&clientsecret=${client_secret}&RegisteredDelivery=true`;

									request.get(smsUrl, (error, response, body) => {
										if (error) throw new Error(error);
										console.log(`Alert Sms Sent To ${contact_number[j].name}`);
									});
								}
								//wait 2 minutes then exit code, in case there are other DLRs being reprocessed
								setTimeout(function(){
									process.exit(); 
								}, 120000);
							}
						}
						 if (error) {
                            console.log(`${index}: ${error}`);
                            return;
                        }
                        						
                    })

                });
        })
    } catch (err) {
        console.log('Database ' + err);
        console.log("Response time after attempt: " + new Date().toISOString().replace(/T/, ' ').replace(/\..+/, ''));
        process.exit();
    }
}
main();

//this will allow the app to run on a different port each time
let _http = http.createServer((req, res) => {
	res.writeHead(200);
	res.end('Hello..!')
}).listen();
console.log("Server listening on " + _http.address().port);
