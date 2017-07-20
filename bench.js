var MongoClient = require('mongodb');
var Agenda = require('./index') // point this to whatever agenda you want to test this on, for now its this feature.

// How many pending jobs to create
var jobs = 2000000;

// Wanted to have a variety of definitions to test the indexes
var definitions = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'];

/*
  ======= To use this script ========
  1. $ node script.js load
        (takes about ~45 seconds, wait until it says done.
  3. $ node script.js agenda
        (this starts agenda)... you can see the output as it processes jobs. (While doing this tail mongodb.log and you'll notice NO recursive locks!)
*/


switch (process.argv[2]) {
  case 'seed':
    seed();
    break;
  case 'agenda':
    agenda();
    break;
  default:
    console.log("specify something to do (seed|agenda)");
    break;
}


function seed() {
  MongoClient.connect('mongodb://localhost:27017/agenda-bench', function (err, db) {
    if (err) throw err;
    db.collection('agendaJobs', function (err, collection) {
      if (err) throw err;
      collection.remove(function (err) {
        if (err) throw err;
        var bulkop = collection.initializeOrderedBulkOp();
        for (var i = 0; i < jobs; i++) {
          if (i % 100000 == 0) {
            console.log('created ' + i + ' jobs');
          }
          bulkop.insert({
            "name": definitions[Math.floor(Math.random() * definitions.length)],
            "data": {
              "job": i
            },
            "type": "normal",
            "priority": 10,
            "nextRunAt": new Date(),
            "lockedAt": new Date(1),
            "disabled": false,
            "lastFinishedAt": null
          });
        };
        console.log('created ' + i + ' jobs');
        console.log('Inserting jobs into database......can take up to a minute.');
        bulkop.execute(function (err, result) {
          if (err) {
            throw err
          } else {
            console.log('Successfully inserted ' + jobs + ' jobs');
          }
          console.log('Creating & building index...');
          const indexes = [{
            "key": {
              "name": 1,
              "disabled": 1,
              "priority": -1,
              "lockedAt": 1,
              "nextRunAt": 1
              ,"lastFinishedAt": 1
            },
            "name": "findAndLockNextJobIndex1"
          }];
          db.collection('agendaJobs').createIndexes(indexes, function (err) {
            console.log('done.');
            process.exit(0);
          });
        });
      });
    });
  });
}



function agenda() {
  var agenda = new Agenda({
    db: {
      address: 'localhost:27017/agenda-bench',
      collection: 'agendaJobs',
      lockLimit: 10,
      defaultLockLimit: 10,
      concurrency: 20
    }
  });
  var i = 0;

  definitions.forEach(function (def) {
    agenda.define(def, function (job, done) {
      console.log('Processing task definition: ' + job.attrs.name + ' (' + ++i + ')');
      done();
    });
  });

  agenda.on('ready', () => agenda.start());
}
