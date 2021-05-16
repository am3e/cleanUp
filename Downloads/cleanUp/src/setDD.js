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


const ddFirstName = "FirstName";
const ddLastName = "LastName";
const ddRegion = "Home_State";
const ddPhoneFields = [
    "Home_Phone",
    "Primary_Phone",
]

console.log("using dd records")
const phoneFields =  ddPhoneFields;
const firstNameField = ddFirstName;
const lastNameField = ddLastName;
const stateRegionField = ddRegion;

const draft = "2021-04-26-newDD.csv";
const newfilename = "DD-set"
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
        
        
        //if it's new contact aka does have a contact id, add new fields
        if ([row["DateAddedToDiscoveryDatabase"]]) {

            const email2 = (row["Email_Business2TypeValidationSupported"] === "Yes") ? row["Email_Business2Type"] : "";
            const email1 = (row["Email_BusinessTypeValidationSupported"] === "Yes") ? row["Email_BusinessType"] : email2;
            const email0 = (row["Email_PersonalTypeValidationSupported"] === "Yes") ? row["Email_PersonalType"] : email1;

            const phone1 = (row["Primary_PhoneDoNotCall"] === "") ? row["Primary_Phone"] : "";
            const phone0 = (row["Home_PhoneDoNotCall"] === "") ? row["Home_Phone"] : phone1;
            const phoneType1 = (row["Primary_PhoneDoNotCall"] === "") ? row["Primary_PhoneLineType"] : "";
            const phoneType0 = (row["Home_PhoneDoNotCall"] === "") ? row["Home_PhoneType"] : phoneType1;

            row["Email*"] = email0 || "";
            row["Phone Number*"] = phone0 || phone1;
            row["Phone Number 2*"] = phone0 ? (phone0 === phone1 ? "" : phone1) : "";
            row["City*"] = row["Home_City"];
            row["State/Region*"] = row["State/Region*"];
            row["Country*"] = row["Country*"];


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