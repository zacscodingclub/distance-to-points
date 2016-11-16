var geolib = require('geolib'),
	   csv = require('fast-csv'),
	    fs = require('fs'),
	  args = process.argv,
	assets = [],
 startTime = Date.now();

var fileName = args[2];
buildAssets(args[3]);

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

function buildAssets(file) {
	var assetStream = fs.createReadStream(file);
	csv
		.fromStream(assetStream)
		.on("data", function(row){
			new Asset(row[0], row[1], row[2]);
		})
		.on("end", function() {
			console.log('\n');
		});
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

function exitMessage() {
	var endTime = Date.now();
	var runTime = (endTime - startTime)/1000
	console.log(`Processing completed in ${runTime}s.\nThe new file is: ${newFileName}`);
}

// Program Main - procedural which could probably be wrapped into something better
csv
	.fromStream(readStream, { headers: true})
	.on("data", function(row){
		populateWriteData(row);
	})
	.on("end", function() {
		buildResultCSV();
		exitMessage();
	});
