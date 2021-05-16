//used to make file sheet comparable
const { DateTime } = require("luxon");
const now = DateTime.local();
const today = DateTime.local(now.year, now.month, now.day);
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const fs = require("fs");
const { pick, groupBy, toUpper } = require("lodash");
const {
    groupByFields,
    uniquePhoneGetter,
    groupByTypeOfFields,
    groupByContactFields,
    loadFile,
    getMapFromHeaders,
    getMapFromHeader,
    parseDate,
} = require("./dataCleanUp.js");

const CSV_DIR = "csv/";


newfilename = "newDD";
const loadFiles = () => {
    const fileList = fs.readdirSync(CSV_DIR);
    const files = fileList.filter((file) => file.match(/DDbatch/gi));
    if (files.length === 0) {
      throw new Error("no files found");
    }
    return files.map( file => ({file, promise: loadFile(file)}));
  };

const processCombine = async () => {
    
    let allRows = [];
    for (const {file, promise} of loadFiles()) {
      console.log(`\nloading ${file}`);
      const rows = await promise;
      allRows = allRows.concat(rows);
      console.log(`${rows.length} rows`);
    }
  
    console.log(`${allRows.length} rows`);
  
    const header = Object.keys(allRows[0]).map(header => ({id:header, title:header}));
    const start = 0;
    const end = allRows.length;
    const mid = Math.ceil(end/2);
    const crsvWriter = createCsvWriter({
        path: `${CSV_DIR}${today.toISODate()}-${newfilename}.csv`,
        header: header,
        });
    await crsvWriter.writeRecords(allRows.slice(start,mid))
    await crsvWriter.writeRecords(allRows.slice(mid,end))
}

processCombine();
