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
const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
const axios = require('axios');
const csv2geojson = require('csv2geojson');

const config = {
  fileName: 'data/Vollpflege.csv',
  fileOutput: 'data/result.csv',
  geoJSONOutput: 'data/result.geojson',
  outputGeoJSON: true,
  streetNameColumn: 'Einrichtung Strasse',
  streetNumberColumn: '',
  PLZColumn: 'Einrichtung PLZ',
  isSameColumnForStreetAndNumber: true,
  geocoderURL: 'https://tsb.ara.uberspace.de/tsb-geocoding/'
};

const counter = {
  missed: 0,
  success: 0,
  total: 0
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
      console.log('...Done writing CSV');
    });
}

function writeGeoJSON(results) {

  let headerArray = [];
  let objectProps = Object.getOwnPropertyNames(results[0]);

  objectProps.forEach(el => {
    headerArray.push({id: el, title: el });
  });
  const csvStringifier = createCsvStringifier({
    header: headerArray
  });
  let stringifiedHeader = csvStringifier.getHeaderString();
  let stringifiedBody = csvStringifier.stringifyRecords(results);
  let stringifiedCSV = stringifiedHeader + stringifiedBody;

  csv2geojson.csv2geojson(stringifiedCSV, {
    latfield: 'lat',
    lonfield: 'lon',
    delimiter: ','
  }, function(err, data) {
    if (err) throw err;

    fs.writeFile(config.geoJSONOutput, data, function(err) {
      if (err) throw err;
      console.log('Done writing GeoJSON.');
    });

  });
}

function main(data) {

  /* Say Hi */
  counter.total = data.length;
  console.log(`Starting to process ${counter.total} entries.`);

  /* Let's add new separate columns for street names and numbers */
  let processed = processCSV(data);

  let axiosArray = [];

  /* Build an Axios call for each entry */
  processed.forEach(el => {
    axiosArray.push(makeAPICalls(el.streetName, el.streetNumber, el[config.PLZColumn]));
  });

  /* Wait for all Axios calls to resolve*/
  Promise.all(axiosArray)
    .then(results => {

      /* Add lat/lon columns with results from Axios call
        You might want to switch lat and lon becasue I'm stupid*/
      processed.forEach((el, i) => {
        el.lon = results[i].lat || '';
        el.lat = results[i].lon || '';
      });

      console.log(`Processed ${processed.length} entries. Found ${counter.missed} errors.`);
      /*Write to disk*/
      writeCSVFile(processed);

      if (config.outputGeoJSON) {
        //for some reason, this doesn't work yet, use csv2geojson CLI
        //writeGeoJSON(processed);
      }
    })
    .catch(console.log);
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

  coords = await getCoordinates(numID);

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
  let result = {};
  let coordinates = await axios.get(`${config.geocoderURL}geo?num=${numID.id}`);


  if (coordinates.data === '') {
    counter.missed++;
    return result;
  } else {
    counter.success++;
    result.lat = coordinates.data.lat;
    result.lon = coordinates.data.lon;

    return result;
  }
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
