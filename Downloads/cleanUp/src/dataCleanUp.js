//reference functions for data ckean up
const fs = require("fs");
const csv = require("csv-parser");
const { performance } = require('perf_hooks');
const { pick, groupBy, flatten, get, union, isFunction, toLower } = require("lodash");
const stripBom = require('strip-bom-stream');
const createCsvWriter = require("csv-writer").createObjectCsvWriter;



//files
const CSV_DIR = "csv/";
const startOfFileName = "/list/gi";

const emailDelimiter = /[ ,;]+/;
const phoneDelimiter = /[,;]+/;
const delimiter = emailDelimiter;

//load one file
const loadFile = file => {
    return new Promise(resolve => {
      const rows = [];
      fs.createReadStream(CSV_DIR + file)
        .pipe(stripBom())
        .pipe(csv())
        .on("data", (row) => rows.push(row))
        .on("end", () => resolve(rows));
    })
  }

//load many files
const loadFiles = () => {
    const fileList = fs.readdirSync(CSV_DIR);
    const files = fileList.filter((file) => file.match(startOfFileName));
    if (files.length === 0) {
      throw new Error("no files found");
    }
    return files.map( file => ({file, promise: loadFile(file)}));
  };


//join files together

//clean phone records
//clean email records

// typeRows.length > 1
//count how many rows exist with a value: email (throughout all available emails)
//count how many rows exist with a value: phone (throughout all available phones)
//count how many rows exist with a value: npn
//count how  many rows exist with a value: repCRD
//find duplicates, how many need to be merged together if they are the same people - compare year of birth, postal and lastname and first two letters of first name, if these don't match export as manual duplicates with contact id of record to keep, record to merge in to and email address for quick find

//file teamp where none of source id is an npn plus count of npn in team p per file type

//find import records that do not exist in hubspot

//check if a field 

function groupByContactFields(typeOfFields, grouping, row) {
    typeOfFields.forEach((typeOfField) => {
      const existingContact = grouping[typeOfField];
      if (!existingContact) {
        grouping[typeOfField] = [row];
      }  else {
        grouping[typeOfField] = union(existingContact, [row]);
      }
    });
  }
  
  const splitRegEx = delimiter;
  function groupByTypeOfFields(fields, row, grouping) {
    fields.forEach((field) => {
      const fieldValue = isFunction(field) ? field(row) : row[field];
      if (fieldValue) {
        const fieldValues = fieldValue.split(splitRegEx);
        groupByContactFields(fieldValues, grouping, row);
      }
    });
  }

  const uniquePhoneGetter = (phone, firstname, lastname) => {
    const firstNameRef = toLower(firstname.trim().substring(0, 2).replace(/[ .,'-]/g,""));
    const lastNameRef = toLower(lastname.trim().replace(/[ .,'-]/g,""));
    const formatPhone = normalizePhone(phone);
    if(formatPhone) {
      const concatPhone = `${formatPhone}-${lastNameRef}${firstNameRef}`;
      return concatPhone;
    }
    return "";
  }
  const normalizePhone = (phone) => {
    const phoneSplitRegEx = /^((\+1)|1)? ?\(?(\d{3})\)?[ .-]?(\d{3})[ .-]?(\d{4})( ?(ext\.? ?|x)(\d*))?$/;
    if (phone) {
      const phoneParts = phone.split(phoneSplitRegEx);
      const formatPhone = `${get(phoneParts,"[3]","")}${get(phoneParts,"[4]","")}${get(phoneParts,"[5]","")}`;
      return formatPhone;
    }
    return "";
  }
  
  const groupByFields = (rows, fields) => {
    let startMS = performance.now();
    const grouping = {};
    rows.forEach( (row, index) => {
      if (index % 1000) {
        const elapsedMS = performance.now() - startMS;
        process.stdout.write(`\r${index} contacts, ${elapsedMS.toFixed(2)} elapsed ms, ${(index * 1000 / elapsedMS).toFixed(2)} contact/ms               `);
      }
      groupByTypeOfFields(fields, row, grouping);
    });
    return grouping;
  }

  const getMapFromHeaders = (rows, key, value) => {
    const map = {};
    rows.forEach((row) => {
      map[row[key]] = row[value];
    });
    return map;
  };
  
  const getMapFromHeader = (rows, key) => {
    const map = {};
    rows.forEach((row) => {
      let lookup = map[row[key]];
      if (!lookup) {
        lookup = map[row[key]] = [];
      }
      lookup.push(row);
    });
    return map;
  };
  
  
  const parseDate = (date) => {
    let result = DateTime.fromISO(date, { zone: "UTC" });
    if (!result.isValid) {
      result = DateTime.fromSQL(date, { zone: "UTC" });
    }
    if (!result.isValid) {
      result = DateTime.fromFormat(date, "yyyy-MM-dd h:m:s", { zone: "UTC" });
    }
    if (!result.isValid) {
      result = DateTime.fromFormat(date, "yyyy-MM-dd h:m", { zone: "UTC" });
    }
    if (!result.isValid) {
      console.error(`date parsing failed for ${date}`);
    }
    return result.setZone("America/Toronto");
  };

  module.exports = {
    groupByFields,
    uniquePhoneGetter,
    groupByTypeOfFields,
    groupByContactFields,
    normalizePhone,
    loadFiles,
    loadFile,
    getMapFromHeaders,
    getMapFromHeader,
    parseDate,
  }