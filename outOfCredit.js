// this scripts sends out an email with a table of information based on data queried from DB

const pg = require('pg');
var nodemailer = require('nodemailer');

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
  var startTime 
  Date.prototype.roundTime = function () {
    this.setMinutes(0, 0, 0)
    return this;
  };

   var endTime = new Date().roundTime().toISOString().replace(/T/, ' ').replace(/\..+/, '');

  var currentQueryDate = new Date(new Date().getFullYear(),new Date().getMonth() , new Date().getDate()).toLocaleDateString();
  console.log(currentQueryDate)
  
if (endTime.includes ("06:00:00")){
  startTime = new Date().subTime(13).toISOString().replace(/T/, ' ').replace(/\..+/, '');
}
else if (endTime.includes ("8:00:00")){
  startTime = new Date().subTime(2).toISOString().replace(/T/, ' ').replace(/\..+/, '');
}
else if (endTime.includes ("12:00:00")){
  startTime = new Date().subTime(4).toISOString().replace(/T/, ' ').replace(/\..+/, '');
}
else if (endTime.includes ("3:00:00")){
  startTime = new Date().subTime(3).toISOString().replace(/T/, ' ').replace(/\..+/, '');
}
else {
  startTime = new Date().subTime(5).toISOString().replace(/T/, ' ').replace(/\..+/, '');
}
console.log(startTime);

console.log(endTime);
async function sendEmail(htmlText){
  var mail = nodemailer.createTransport({
    host: "host",
    port: port,
    secure: false,
  auth: {
    user: 'user',
    pass: 'pass'
  }
});
//
const recepients = [
    "recipient1@email.com",
    "recipient2@email.com",
    "recipient3@email.com",
    "recipient4@email.com"
]

var mailOptions = {
   from: 'sender-email',
   to : `${recepients}`,
   cc: 'optional',
   subject: 'EMAIL SUBJECT',
   html: htmlText
}
 
mail.sendMail(mailOptions, function(error, info){
      if (error) {
        console.log(error);
      } else {
        console.log('Email sent: ' + info.response);
      }
});
}

addComma = new Intl.NumberFormat('en-US'); 
const createRow = (item) => `
  <tr style="background-color: #FFE4E1 !important;">
    <td style="text-align:center;">${item.column1}</td>
	<td style="text-align:center;">${item.column2}</td>
  </tr>
`;
const createTable = (rows) => `
  <table style="border: 1px solid black !important;">
    <tr style="background-color: #6495ED !important; text-align:center;">
        <th style="text-align:center;">column1 heading</td>
		<th style="text-align:center;">column2 heading</td>
    </tr>
    ${rows}
  </table>
`;
const createHtml = (table) => `
  <html>
    <head>
      <style>
        table {
          width: 100%;
        }
        tr {
          text-align: left;
          border: 1px solid black !important;
        }
        th, td {
          padding: 10px;
        }
        tr:nth-child(odd) {
          background-color: #C0C0C0 !important;
        }
        tr:nth-child(even) {
          background-color: #FFE4E1 !important;
        }
        .no-content {
          background-color: #F08080 !important;
        }
      </style>
    </head>
    <body>
      <p>Hello All, </p>
      <p>Report is for within ${startTime} and ${endTime}</p>
      <br>
      ${table}
    </body>
  </html>
`;

const main = async (req, res) => {
  const queryData = `SELECT column1, count(*) AS "column2 heading"
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
    
    console.log("/n");
	console.log(dbResponse);
	console.log("/n");
    const records = dbResponse.map(createRow).join('');
    const table = createTable(records);
    const html = createHtml(table);
    await sendEmail(html);
  } catch (err) {
    console.log('Database ' + err);
    console.log("Response time after attempt: " + new Date().toISOString().replace(/T/, ' ').replace(/\..+/, ''));
    process.exit();
  }
}
main();
