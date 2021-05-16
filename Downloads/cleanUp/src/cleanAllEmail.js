const { DateTime } = require("luxon");
const now = DateTime.local();
const { uniq, flatMap, toLower } = require("lodash");
const today = DateTime.local(now.year, now.month, now.day);
const todayHS = `${now.month}/${now.day}/${now.year}`;

const { performance } = require('perf_hooks');
const { pick, groupBy, flatten, forEach, get, union } = require("lodash");
const stripBom = require('strip-bom-stream');
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const {
    groupByFields,
    normalizePhone,
    groupByTypeOfFields,
    groupByContactFields,
    loadFiles,
    loadFile,
} = require("./dataCleanUp.js");


const CSV_DIR = "csv/";
// const startOfFileName = "/list/gi";
// const emailFile = "hubspot-crm-exports-associate-emails-2021-03-06.csv";

const addiFile = "export-contacts-full.csv";
const seFile = "stripe-email.csv";
const eFile = "email.csv";
const aeFile = "associate-emails.csv";
const alleFile = "all-emails.csv"
const weFile = "work-email.csv"

const combineEmails = async () => {
    console.log("loading files");
    const [add, se, e, ae, alle, we
        ] = await Promise.all([
        loadFile(addiFile),
        loadFile(seFile),
        loadFile(eFile),
        loadFile(aeFile),
        loadFile(alleFile),
        loadFile(weFile),
    ]);

foundEmails = [];

    // using contact id, for all the fields that being with "email previous value (" find all unique in all emails
    // then create a new "all emails" with a unique list of all emails to add to all emails for future deduping

    const stripeEmailHistoryIndex = se.reduce( (index, row) => index[row["Contact ID"]] = row, {});
    const emailHistoryIndex = e.reduce( (index, row) => index[row["Contact ID"]] = row, {});
    const associateEmailHistoryIndex = ae.reduce( (index, row) => index[row["Contact ID"]] = row, {});
    const allEmailHistoryIndex = alle.reduce( (index, row) => index[row["Contact ID"]] = row, {});
    const workEmailHistoryIndex = we.reduce( (index, row) => index[row["Contact ID"]] = row, {});


    const findEmails = (row = {}) => Object.entries(row)
        .map(([header, value]) => toLower(value))
        .filter((value) => value && value.includes && value.includes("@"))
        .flatMap((value) => value.split(/[, ;]+/));

    add.forEach( row => {
        const emails = uniq([...findEmails(row), 
            ...findEmails(stripeEmailHistoryIndex[row["Contact ID"]]),
            ...findEmails(emailHistoryIndex[row["Contact ID"]]), 
            ...findEmails(associateEmailHistoryIndex[row["Contact ID"]]),
            ...findEmails(allEmailHistoryIndex[row["Contact ID"]]),
            ...findEmails(workEmailHistoryIndex[row["Contact ID"]]) 
        ]);
        console.log(emails);
        // foundEmails.push({"Found Emails": emails, 'Contact ID': row["Contact ID"]})
        //         return true;
        row["Found Emails"] = emails;
        
    });

    const header = Object.keys(add[0]).map(header => ({id:header, title:header})); 

    // headers = [
    //     { id: "Contact ID", title: "Contact ID" },
    //     { id: "Found Emails", title: "Found Emails" },
    //   ];  

        const start = 0;
        const end = add.length;
        const mid = Math.ceil(end/2);
        const crsvWriter = createCsvWriter({
            path: `${CSV_DIR}${today.toISODate()}-HSredo.csv`,
            header: header,
            });
        await crsvWriter.writeRecords(add.slice(start,mid))
        await crsvWriter.writeRecords(add.slice(mid,end))
}

combineEmails();