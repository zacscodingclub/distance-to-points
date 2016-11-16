var geolib = require('geolib'),
	   csv = require('fast-csv'),
	    fs = require('fs');

var assets = [];

// **** THIS IS HARD CODED!!
var fileName = "./test/small_data.csv";
var asset1 = new Asset("RF Box 1", 33.529053, -112.296791);
// Each asset created will add a new column to the final CSV
// var asset2 = new Asset("RF Box 2", 33.529003, -112.296783);
// var asset3 = new Asset("RF Box 3", 33.666313, -112.221207);
// **** THIS IS HARD CODED!!

var readStream = fs.createReadStream(fileName);
var newFileName = formatNewFileName(fileName);
var writeStream = fs.createWriteStream(newFileName);
var writeData = [];

// Asset constructor i.e. RF Box, phone
function Asset(name, latitude, longitude) {
	this.name = name;
	this.latitude = latitude;
	this.longitude = longitude;
	assets.push(this);
}

// Helper Functions
function distanceBetweenPoints(lat1, long1, lat2, long2) {
	// args for .getDistance(point1, point2, accuracy, precision= # of decimal places)
	return geolib.getDistance(
			{latitude: lat1, longitude: long1},
			{latitude: lat2, longitude: long2},
			15,
			3
		)
}

function formatNewFileName(fileName) {
	var newName = fileName.split('.csv');
	newName[1] ='updated.csv';

	return newName.join('_');
}

function formatNewColumnNames(assets) {
	return assets.map(function(asset) {
		var assetName = asset.name.split(" ").join("_");
		return `distance_to_${assetName}`;
	});
}

function populateWriteData(row) {
	var rowLongitudeFix = row.LONGITUDE * -1;
	var distances = [];
	var newColumnNames = formatNewColumnNames(assets);

	assets.forEach(function(asset) {
		distances.push(distanceBetweenPoints(asset.latitude, asset.longitude, row.LATITUDE, rowLongitudeFix))
	});

	var newRow = {
				  'index': row.INDEX,
		   'UTC DateTime': row["UTC DATE"] + " " + row["UTC TIME"],
			   'latitude': row.LATITUDE,
			  'longitude': rowLongitudeFix
		  }

	for(var i=0; i < distances.length; i ++) {
		newRow[newColumnNames[i]] = distances[i];
	}

	writeData.push(newRow);
}

function buildResultCSV() {
	csv
		.write(writeData, { headers: true})
		.pipe(writeStream);
}

// Program Main - procedural which could probably be wrapped into something better
csv
	.fromStream(readStream, { headers: true})
	.on("data", function(row){
		populateWriteData(row);
	})
	.on("end", function() {
		buildResultCSV();
		console.log(`Processing complete.\nThe new file is: ${newFileName}`);
	});
