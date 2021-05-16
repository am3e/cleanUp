const { DateTime } = require("luxon");
const now = DateTime.local();
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

// fn	ln	phone1	email1	email2	dob	gen	ct	st	zip1	zip2	phone2	email3

const CSV_DIR = "csv/";
// const startOfFileName = "/list/gi";
const hubspotFile = `${today.toISODate()}-eric-set.csv`;
const datadiscoveryFile = `${today.toISODate()}-DD-set.csv`;

const hsEmailFields = [
    "email1",
    "email2",
    "email3",
]
const ddEmailFields = [
    "Email_BusinessType", 
    "Email_Business2Type", 
    "Email_PersonalType"
]

const hsPhoneFields = [
    "phone1-Ref",
    "phone2-Ref",
]
const ddPhoneFields = [
    "Home_Phone-Ref",
    "Primary_Phone-Ref",
]

const processFiles = async () => {
    console.log("loading files");
    const [hubspot, datadiscovery, 
        ] = await Promise.all([
        loadFile(hubspotFile),
        loadFile(datadiscoveryFile),
    ]);


    console.log("grouping fields by hubspot file");
    const hubspotByEmail = groupByFields(hubspot, hsEmailFields);
    const hubspotByPhone = groupByFields(hubspot, hsPhoneFields);

    console.log()
    const existsWithEmail = [];
    const existsWithPhone = [];

    const newProspectCheck = datadiscovery.filter( ddContact => {
        //return false if theres a duplicate in hubspot
        const matchingEmailField = ddEmailFields.find(field => ddContact[field] && hubspotByEmail[ddContact[field]])
        if (matchingEmailField) {
            const hubspotContact = hubspotByEmail[ddContact[matchingEmailField]][0];
            if (!hubspotContact["NPN"]) {
                existsWithEmail.push({Match: "Email", NPN: ddContact["NPN"], 'Contact ID': hubspotContact["Contact ID"]})
                return false;
            }
        }
        const matchingPhoneField = ddPhoneFields.find(field => ddContact[field] && hubspotByPhone[ddContact[field]])
        if (matchingPhoneField) {
            const hubspotContact = hubspotByPhone[ddContact[matchingPhoneField]][0];
            if (!hubspotContact["NPN"]) {
                existsWithEmail.push({Match: "Phone", NPN: ddContact["NPN"], 'Contact ID': hubspotContact["Contact ID"]})
                return false;
            }
        }
        return true;
    });
    newProspectCheck.forEach( contact => {
        contact["Lifestyle Stage"] = "Lead";
        contact["Lead Status"] = "New";
        contact["DD_Updated_Date"] = `${todayHS}`;
        contact["Contact Source"] = "DD";
        // contact["Contact Owner"] = "lukasz@planswell.com";
        contact["FirstName_DD"] = contact["FirstName"];
        contact["LastName_DD"] = contact["LastName"];
        contact["City"] = contact["City*"];
        contact["State/Region"] = contact["State/Region*"];
        contact["Phone Number"] = contact["Phone Number*"];
        contact["Phone Number 2"] = contact["Phone Number 2*"];


    });

    const findHubspotDupes = (fields) => {
        const grouped = groupByFields(hubspot,fields);
        return Object.entries(grouped)
        .filter(([fieldValue,contacts]) => fieldValue && contacts.length > 1);
    }

    const fieldDefaults = {Email: false, Phone: false, NPN: false};
    const indexDuplicates = (duplicateIndex, entries, score, fieldName) => {
        entries.forEach(([fieldValue,contacts]) => {
            const contact = contacts.find(contact => duplicateIndex[contact["Contact ID"]]);
            if (!contact) {
                const primaryContact = contacts[0];
                duplicateIndex[primaryContact["Contact ID"]] = {...fieldDefaults, score, duplicates: contacts, [fieldName]:true }
            } else {
                const indexedContact = duplicateIndex[contact["Contact ID"]];
                indexedContact.score += score;
                indexedContact.duplicates = union(indexedContact.duplicates, contacts);
                indexedContact[fieldName] = true;
            }
        }); 
    }
    const duplicateIndex = {}
    indexDuplicates(duplicateIndex,findHubspotDupes(hsEmailFields), 2, "Email");
    indexDuplicates(duplicateIndex,findHubspotDupes(hsPhoneFields), 1, "Phone");

    const scoredDuplicates = Object.values(duplicateIndex).map(indexedContact => {
        const {duplicates, ...remainingFields} = indexedContact;
        return {
            ...remainingFields, 
            ...Object.fromEntries(duplicates.map((contact, index) => [`C${index}`, contact["Contact ID"]]))
            };
        }
    )

    // console.log(scoredDuplicates);

    createCsvWriter({ 
        path: `${CSV_DIR}${today.toISODate()}-scoredDuplicates.csv`,
        header: [
            { id: "Email", title: "Email" },
            { id: "Phone", title: "Phone" },
            { id: "score", title: "score" },
            { id: "FullName", title: "FullName" },
            { id: "C0", title: "C1" },
            { id: "C1", title: "C2" },
            { id: "C2", title: "C3" },
            { id: "C3", title: "C4" },
            { id: "C4", title: "C5" },
            { id: "C5", title: "C6" },
            ],
        }).writeRecords(scoredDuplicates);

    const ddHeader = Object.keys(newProspectCheck[0]).map(header => ({id:header, title:header}));
    headers = [
        { id: "Contact ID", title: "Contact ID" },
        { id: "Match", title: "Match" },
      ];  
    createCsvWriter({
        path: `${CSV_DIR}${today.toISODate()}-fbList.csv`,
        header: ddHeader,
        }).writeRecords(newProspectCheck);
    createCsvWriter({
        path: `${CSV_DIR}${today.toISODate()}-existsWithEmail.csv`,
        header: headers,
        }).writeRecords(existsWithEmail);

    console.log("On Import = do not import phonetype and hometype, change phone number to 1 and bdriarep = dually registered, check state and region and phone number and lead status, contact source, contact owner")
}

//within hubspot look at npn, email, phone (each record )
  

  //note to self - you did not set the files yet
processFiles();