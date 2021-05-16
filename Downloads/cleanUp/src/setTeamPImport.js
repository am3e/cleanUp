//for team p to call list (export from hubspot and use script to import to vici)
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
    loadFiles,
    loadFile,
    getMapFromHeaders,
    getMapFromHeader,
    parseDate,
} = require("./dataCleanUp.js");

const draft = "vici-export.csv";
const newfilename = "vici-teamp-import"
const reference = "references.csv";
const CSV_DIR = "csv/";

const setList = async () => {
    //load file

    console.log("loading files");
    const [fileRows, referenceRows, 
        ] = await Promise.all([
        loadFile(draft),
        loadFile(reference),
    ]);

    console.log(fileRows.length);

    fileRows.forEach(row => {
        		
            row["email"] = row["Email"];
            row["address1"] = row["Email_Business2Type"];
            row["address2"] = row["Email_BusinessType"];
            row["address3"] = row["Email_PersonalType"];
            row["city"] = row["City"];
            row["state"] = row["State/Region"];
            row["country_code"] = row["Country/Region"] || "USA";
            row["source_id"] = row["NPN"];
            row["phone_number"] = row["Phone Number"] ? row["Phone Number"] : row["Phone Number 2"];
            row["alt_phone"] = row["Phone Number"] !== "" ? row["Phone Number 2"] : "";
            row["full_name"] = `${row["First Name"]} ${row["Last Name"]}`;
            row["first_name"] = `${row["First Name"]}`;
            row["last_name"] = `${row["Last Name"]}`;

            row["hubspot_link"] = `https://app.hubspot.com/contacts/8068816/contact/${row["Contact ID"]}`;
            row["owner"] = row["Contact owner"];
    });
    console.log("Team P Export")
    //write a new file with all values
    headers = [
        { id: "email", title: "email" },
        { id: "address1", title: "address1" },
        { id: "address2", title: "address2" },
        { id: "address3", title: "address3" },
        { id: "city", title: "city" },
        { id: "state", title: "state" },
        { id: "country_code", title: "country_code" },
        { id: "source_id", title: "source_id" },
        { id: "phone_number", title: "phone_number" },
        { id: "alt_phone", title: "alt_phone" },
        { id: "full_name", title: "full_name" },
        { id: "first_name", title: "first_name" },
        { id: "last_name", title: "last_name" },

        { id: "hubspot_link", title: "hubspot_link" },
        { id: "owner", title: "owner" },
      ];    
      
    createCsvWriter({
        path: `${CSV_DIR}${today.toISODate()}-${newfilename}.csv`,
        header: headers,
        }).writeRecords(fileRows);    
}


setList();