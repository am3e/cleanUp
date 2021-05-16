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
    loadFiles,
    loadFile,
    getMapFromHeaders,
    getMapFromHeader,
    parseDate,
} = require("./dataCleanUp.js");


const hsFirstName = "First Name";
const hsLastName = "Last Name";
const hsRegion = "State/Region";
const hsPhoneFields = [
    "Phone Number",
    "Phone Number 2",
    "Home_Phone",
    "Primary_Phone",
    "Mobile Phone Number",
]

const draft = "export-contacts-full.csv";
const newfilename = "HS-set"
const reference = "references.csv";
const CSV_DIR = "csv/";

// /*
console.log("using hubspot records")
const phoneFields =  hsPhoneFields;
const firstNameField = hsFirstName;
const lastNameField = hsLastName;
const stateRegionField = hsRegion;
// */ 

const setList = async () => {
    //load file

    console.log("loading files");
    const [fileRows, referenceRows, 
        ] = await Promise.all([
        loadFile(draft),
        loadFile(reference),
    ]);

    console.log(fileRows.length);

    const indexZipCodeToState = getMapFromHeaders(referenceRows, "ZipCode", "State Full Name");
    const indexStateCodeToState = getMapFromHeaders(referenceRows, "State Code", "State Full Name");
    const indexAreaCodeToState = getMapFromHeaders(referenceRows, "Area Code", "State Name");
    const indexZipCodeToCountry = getMapFromHeaders(referenceRows, "ZipCode", "Country");
    const indexStateCodeToCountry = getMapFromHeaders(referenceRows, "State Code", "Country");
    const indexAreaCodeToCountry = getMapFromHeaders(referenceRows, "Area Code", "CountryCode");

    fileRows.forEach(row => {
        phoneFields.forEach(field => {
            row[`${field}-Ref`] = uniquePhoneGetter(row[field],row[firstNameField],row[lastNameField]);
        })
            
        //clean region using phone number or zipcode
            const areaCodeState1 = parseInt(uniquePhoneGetter(row["Home_Phone"],row[firstNameField],row[lastNameField]).substring(0, 3));
            const areaCodeState2 = parseInt(uniquePhoneGetter(row["Phone Number"],row[firstNameField],row[lastNameField]).substring(0, 3));

            if (row["Home_ZipCode"] && indexZipCodeToState[row["Home_ZipCode"]]) {
                row["State/Region*"] = indexZipCodeToState[row["Home_ZipCode"]];
                row["Country*"] = indexZipCodeToCountry[row["Home_ZipCode"]];

            } else if (row["Home_Phone"] && indexAreaCodeToState[areaCodeState1]) {
                row["State/Region*"] = indexAreaCodeToState[areaCodeState1];
                row["Country*"] = indexAreaCodeToCountry[areaCodeState1];

            } else if (row["Home_State"] && indexStateCodeToState[toUpper(row["Home_State"]).trim()]) {
                row["State/Region*"] = indexStateCodeToState[toUpper(row["Home_State"]).trim()];
                row["Country*"] = indexStateCodeToCountry[toUpper(row["Home_State"]).trim()];

            } else if (row["Phone Number"] && indexAreaCodeToState[areaCodeState2]) {
                row["State/Region*"] = indexAreaCodeToState[areaCodeState2];
                row["Country*"] = indexAreaCodeToCountry[areaCodeState2];

            } else if (row[stateRegionField].length === 2 && indexStateCodeToState[toUpper(row[stateRegionField]).trim()]) {
                row["State/Region*"] = indexStateCodeToState[toUpper(row[stateRegionField]).trim()];
                row["Country*"] = indexStateCodeToCountry[toUpper(row[stateRegionField]).trim()];

            } else if (row[stateRegionField].length > 2 && [row[stateRegionField]]) {
                row["State/Region*"] = row[stateRegionField];

            }
        
    });
    //write a new file with all values
    const header = Object.keys(fileRows[0]).map(header => ({id:header, title:header})); 

    const start = 0;
    const end = fileRows.length;
    const mid = Math.ceil(end/2);
    const crsvWriter = createCsvWriter({
        path: `${CSV_DIR}${today.toISODate()}-${newfilename}.csv`,
        header: header,
        });
    await crsvWriter.writeRecords(fileRows.slice(start,mid))
    await crsvWriter.writeRecords(fileRows.slice(mid,end))
}


setList();