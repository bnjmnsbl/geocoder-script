/*
To use this script you should:
- Set the right input file in config
- Set the right names for the columns
- Check that the geocoder URL is still valid

This script only works for Berlin!
*/

'use strict';

const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const axios = require('axios');

let config = {
  fileName: 'data/Vollpflege.csv',
  fileOutput: 'data/result.csv',
  streetNameColumn: 'Einrichtung Strasse',
  streetNumberColumn: '',
  PLZColumn: 'Einrichtung PLZ',
  isSameColumnForStreetAndNumber: true,
  geocoderURL: 'https://tsb.ara.uberspace.de/tsb-geocoding/'
};

let counter = {
  missed: 0,
  success: 0
};

function readCSVFile (file, delimiter) {

  let results = [];

  fs.createReadStream(file)
    .pipe(csv({separator: delimiter}))
    .on('data', (data) => results.push(data))
    .on('end', () => {
      (main(results));
    });

}

function writeCSVFile (results) {

  let headerArray = [];
  let objectProps = Object.getOwnPropertyNames(results[0]);

  objectProps.forEach(el => {
    headerArray.push({id: el, title: el });
  });

  const csvWriter = createCsvWriter({
    path: config.fileOutput,
    header: headerArray
  });

  csvWriter.writeRecords(results)
    .then(()=> {
      console.log('...Done');
    });
}

function main(data) {

  /* Say Hi */
  console.log(`Starting to process ${data.length} entries.`);

  /* Let's and new separate columns for street names and numbers */
  let processed = processCSV(data);

  let axiosArray = [];

  /* Build an Axios call for each entry */
  processed.forEach(el => {
    axiosArray.push(makeAPICalls(el.streetName, el.streetNumber, el[config.PLZColumn]));
  });

  /* Wait for all Axios calls to resolve*/
  Promise.all(axiosArray)
    .then(results => {

      /* Add lat/lon columns with results from Axios call*/
      processed.forEach((el, i) => {
        el.lat = results[i].lat || '';
        el.lon = results[i].lon || '';
      });

      console.log(`Processed ${processed.length} entries. Found ${counter.missed} errors.`);
      /*Write to disk*/
      writeCSVFile(processed);
    });
}


function processCSV(data) {

  if (config.isSameColumnForStreetAndNumber) {
    data.forEach(el => {
      //Splits string at first occurence of a digit
      let streetArray = el[config.streetNameColumn].split(/(\d+)/);

      el.streetName = streetArray[0];

      //Remove first Element, keep Array
      streetArray.shift();

      //Join and replace whitespace to have a single number
      el.streetNumber = streetArray.join('').replace(/\s/g,'');

    });

    return data;
  }
}

async function makeAPICalls (name, number, plz) {

  let coords = {};

  const streetID = await getStreetID(name, plz);

  if (streetID === undefined) {
    counter.missed++;
    return coords;
  }

  const numID = await getNumID(streetID, number);

  if (numID === undefined) {
    counter.missed++;
    return coords;
  }

  const coordinates = await getCoordinates(numID);

  if (coordinates.data === '') {
    counter.missed++;
    return ('No coordinates found for ' + name + ' ' + number);
  } else {
    counter.success++;
    coords.lat = coordinates.data.lat;
    coords.lon = coordinates.data.lon;

  }

  logInfo();
  return coords;
}

async function getStreetID(name, plz) {

  let result = await axios.get(`${config.geocoderURL}street?street=${name}`);
  let reply;

  if (result.data[0] === undefined) {
    reply = undefined;
  } else if (result.data.length > 1) {
    let a = result.data.filter(el => {
      return el.plz === parseInt(plz);
    });

    if (a.length === 1) {
      reply = a[0].id;
    } else {
      console.log(`Found no match for ${name} and ${plz}`);
      reply = undefined;
    }
  } else {
    reply = result.data[0].id;
  }

  return reply;
}

async function getNumID(identifier, number) {
  let numID = await axios.get(`${config.geocoderURL}num?street=${identifier}`);
  let id = numID.data.find(el => el.num === number);

  if (id === undefined) {
    //try again with refined number
    let newNumber = refineNumber(number);
    id = numID.data.find(el => el.num === newNumber);
  }

  return id;
}

async function getCoordinates(numID) {

  let result = axios.get(`${config.geocoderURL}geo?num=${numID.id}`);
  return result;

}

function refineNumber(number) {
  //this function tries to clean a street number that was not found in DB

  let cleanNumber = number.split('-')[0];
  cleanNumber = cleanNumber.split('/')[0];

  cleanNumber = cleanNumber.replace('a','');
  cleanNumber = cleanNumber.replace('b','');
  cleanNumber = cleanNumber.replace('A','');
  cleanNumber = cleanNumber.replace('B','');
  cleanNumber = cleanNumber.trim();

  return cleanNumber;
}

function logInfo() {
  console.log(`Processed ${counter.missed + counter.success} entries`);
}

//Start reading the file and initiate the process
readCSVFile(config.fileName, ';');
