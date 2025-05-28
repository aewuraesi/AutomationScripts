require('dotenv').config();
const cron = require('node-cron');
const ExcelJS = require('exceljs');
const fs = require('fs');
const nodemailer = require('nodemailer');
const request = require('request');
const { format } = require('date-fns');

// Calculate previous month date range and label
const getPrevMonthRange = () => {
  const now = new Date();
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 0, 23, 59, 59, 999));
  return {
    prevMonthStart: start.toISOString(),
    prevMonthEnd: end.toISOString(),
    label: format(start, 'MMMM_yyyy')
  };
};

// Elasticsearch aggregation query
const queryES = ({ prevMonthStart, prevMonthEnd }) => {
  const body = {
    size: 0,
    query: {
      bool: {
        must: [
          { term: { channel: 'sms' } },
          {
            range: {
              createdAt: {
                gte: prevMonthStart,
                lte: prevMonthEnd
              }
            }
          }
        ]
      }
    },
    aggs: {
      by_sender: {
        terms: { field: 'sender.keyword', size: 1000 },
        aggs: {
          by_gateway: {
            terms: { field: 'gateway.keyword', size: 1000 },
            aggs: {
              by_status: {
                terms: { field: 'status.keyword', size: 1000 },
                aggs: {
                  messagecount: {
                    value_count: { field: '_id' }
                  }
                }
              }
            }
          }
        }
      }
    }
  };

  return new Promise((resolve, reject) => {
    request.post({
      url: `${process.env.ES_PRODUCER_URL}/_search`,
      body,
      json: true
    }, (error, response, body) => {
      if (error) return reject(error);
      resolve(body);
    });
  });
};

// Format Elasticsearch result into flat row structure
const flattenAggs = (data) => {
  const rows = [];
  if (!data?.aggregations?.by_sender?.buckets) return rows;

  for (const sender of data.aggregations.by_sender.buckets) {
    for (const gateway of sender.by_gateway.buckets) {
      let gatewayTotal = 0;
      gateway.by_status.buckets.forEach(status => {
        gatewayTotal += status.messagecount.value;
      });
      for (const status of gateway.by_status.buckets) {
        const percentage = (status.messagecount.value / gatewayTotal) * 100;
        rows.push({
          sender: sender.key,
          gateway: gateway.key,
          status: status.key,
          count: status.messagecount.value,
          percent: percentage.toFixed(6) + '%'
        });
      }
    }
  }
  return rows;
};

// Send report via email
const sendEmail = async (filePath, label) => {
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: parseInt(process.env.SMTP_PORT || '587'),
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS
    }
  });

  const mailOptions = {
    from: process.env.EMAIL_FROM,
    to: process.env.EMAIL_TO?.split(',') || [],
    cc: process.env.EMAIL_CC?.split(',') || [],
    subject: `Monthly Producer SMS Stats - ${label.replace('_', ' ')}`,
    text: `Attached is the Producer SMS report for ${label.replace('_', ' ')}.`,
    attachments: [
      {
        filename: `Producer_SMS_Stats_${label}.xlsx`,
        content: fs.createReadStream(filePath)
      }
    ]
  };

  try {
    const info = await transporter.sendMail(mailOptions);
    console.log('✅ Email sent: ' + info.response);
  } catch (error) {
    console.error('❌ Error sending email:', error);
  }
};

// Main reporting function
const runProducerReport = async () => {
  const { prevMonthStart, prevMonthEnd, label } = getPrevMonthRange();
  console.log(`📦 Running Producer SMS Report for ${label}`);

  try {
    const rawResult = await queryES({ prevMonthStart, prevMonthEnd });
    const rows = flattenAggs(rawResult);
    if (rows.length === 0) {
      console.log('⚠️ No data found.');
      return;
    }

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Producer Stats');
    sheet.columns = [
      { header: 'Sender', key: 'sender', width: 20 },
      { header: 'Gateway', key: 'gateway', width: 25 },
      { header: 'Status', key: 'status', width: 20 },
      { header: 'Count', key: 'count', width: 15 },
      { header: 'Percent', key: 'percent', width: 20 }
    ];

    rows.forEach(row => sheet.addRow(row));

    let i = 2;
    while (i < rows.length + 2) {
      const start = i;
      const sender = sheet.getCell(`A${i}`).value;
      const gateway = sheet.getCell(`B${i}`).value;

      while (i < rows.length + 2 && sheet.getCell(`A${i}`).value === sender) i++;
      if (i - start > 1) {
        sheet.mergeCells(`A${start}:A${i - 1}`);
        sheet.getCell(`A${start}`).alignment = { vertical: 'middle', horizontal: 'center' };
      }

      let j = start;
      while (j < i) {
        const gStart = j;
        const gVal = sheet.getCell(`B${j}`).value;
        while (j < i && sheet.getCell(`B${j}`).value === gVal) j++;
        if (j - gStart > 1) {
          sheet.mergeCells(`B${gStart}:B${j - 1}`);
          sheet.getCell(`B${gStart}`).alignment = { vertical: 'middle', horizontal: 'center' };
        }
      }
    }

    const fileName = `Producer_SMS_Stats_${label}.xlsx`;
    await workbook.xlsx.writeFile(fileName);
    await sendEmail(fileName, label);
  } catch (err) {
    console.error('❌ Failed to generate producer report:', err);
  }
};

// Schedule monthly on the 1st at 02:00 AM
cron.schedule('0 2 1 * *', () => {
  runProducerReport();
  console.log('✅ Scheduled monthly producer report triggered');
});

// Run immediately
runProducerReport();
console.log('📊 Producer SMS reporting service started');
