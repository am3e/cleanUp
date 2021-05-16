//fix timestamps and stages and sources of deals for clean reporting
//date stamps changes

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

const ref = "references.csv";
const hsactivities = "20210321-activities.csv";

const newfilename = "dealCleanUp-earliestDate"
const CSV_DIR = "csv/";


const checkDeal = async () => {
    //load file

    console.log("loading files");
    const [refRows, hsactivitiesRows
        ] = await Promise.all([
        loadFile(ref),
        loadFile(hsactivities),
    ]);

    const findEarliestDate = (activities, dateField) => {
        return activities.reduce((earliestActivity, nextActivity) => {
            if (nextActivity[dateField] < earliestActivity[dateField]) {
                return nextActivity;
            }
            return earliestActivity;
        }, activities[0]);
    }

    const earliestDate = [];

    const activitesByContactId = Object.values(groupBy(hsactivitiesRows, hsactivitiesRows["Contact ID"]));
    // const assignedOn = row["Assigned On"];
    const earliestContactActivities = activitesByContactId.map(activitiesForAContact => findEarliestDate(activitiesForAContact, "contact"));
    const earliestBookedActivities = activitesByContactId.map(activitiesForAContact => findEarliestDate(activitiesForAContact, "booked"));
    const earliestHeldActivities = activitesByContactId.map(activitiesForAContact => findEarliestDate(activitiesForAContact, "held"));

    console.log(earliestHeldActivities);
    earliestDate.push({'Contact ID': hsactivitiesRows["Contact ID"], 'Contact Made': earliestContactActivities, 'Demo Booked': earliestBookedActivities, 'Demo Held': earliestHeldActivities});


    // const today = 44276;
    // const subscriptionStart = row["Date of First Successful Payment"];


    //first find the earliest date of all activities
    //second see if the deal timestamps are in order

    // dealRows.forEach(row => {
    //     row["Stage 5: Closed Won Timestamp"] = subscriptionStart ? subscriptionStart : "";
    //     row["Stage 4: Demo Held Timestamp"] = 
    //         earliestHeldActivities > subscriptionStart ? row["Stage 5: Closed Won"] : 
    //         earliestHeldActivities > today ? "" : earliestHeldActivities;
    //     row["Stage 3: Demo Booked Timestamp"] = 
    //         earliestBookedActivities > earliestHeldActivities ? row["Stage 4: Demo Held"] : earliestBookedActivities;
    //     row["Stage 2: Contact Made Timestamp"] = earliestContactActivities > earliestBookedActivities ? row["Stage 3: Demo Booked"] : earliestContactActivities;
    //     row["Stage 1: Assigned Timestamp"] = assignedOn > earliestContactActivities ? row["Stage 2: Contact Made"] : assignedOn;

    //     row["Deal Stage"] = 
    //         row["Stage 5: Closed Won Timestamp"] ? "Closed Won" :
    //         row["Stage 4: Demo Held Timestamp"] ? "Demo Held" :
    //         row["Stage 3: Demo Booked Timestamp"] ? "Demo Booked" :
    //         row["Stage 2: Contact Made Timestamp"] ? "Contact Made" :
    //         row["Stage 1: Assigned Timestamp"] ? "Assigned" :
    //         row["Stage 0: Closed Lost Timestamp"] ? "Closed Lost" : "";

    // });


    //write a new file with all values
    const header = [
        { id: "Contact ID", title: "Contact ID" },
        { id: "Contact Made", title: "Contact Made" },
        { id: "Demo Booked", title: "Demo Booked" },
        { id: "Demo Held", title: "Demo Held" },

    ]; 

    const crsvWriter = createCsvWriter({
        path: `${CSV_DIR}${today.toISODate()}-${newfilename}.csv`,
        header: header,
        });
    await crsvWriter.writeRecords(earliestDate);
}


checkDeal();