const csv = require('fast-csv');
const fs = require('fs');
const { finished } = require('stream');


var Mapper = function (options) {
    this._options = options;
    if (!this._options.store) {
        this._options.store = 'MemStore';
    }
};


Mapper.prototype.resultStream = function (path, output, done) {
    let t0 = new Date();
    var stoptimes = fs.createReadStream('gtfs/stop_times.txt', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true, quote: '"' }))
        .on('error', error => console.error(error));

    var options = this._options;
    var stoptimesdb = Store(output + '/.stoptimes', options.store);
    let format = options['format'];
    let ext = null;
    if (!format || ['json', 'mongo', 'jsonld', 'mongold'].indexOf(format) >= 0) {
        await appendLineBreaks();
        ext = 'json';
    } else if (format === 'csv') {
        ext = 'csv';
    } else if (format === 'turtle') {
        await removePrefixes();
        ext = 'ttl';
    } else if (format === 'ntriples') {
        ext = 'n3';
    }

    try {
        await exec(`cat raw_* > linkedStopTimes.${ext}`, { cwd: output });
        let t1 = new Date();
        console.error('linkedStopTimes.' + ext + ' File created in ' + (t1.getTime() - t0.getTime()) + ' ms');
        await del([
            output + '/.stoptimes',
            output + '/raw_*'
        ],
            { force: true });
        done(`${output}/linkedStopTimes.${ext}`);
    } catch (err) {
        throw err;
    }
    createIndex('trip_id', stoptimes, stoptimesdb).then(() => finished());

}

async function createIndex(id, stream, db) {
    for await (const data of stream) {
      if (data[id]) {
        await db.set(data[id], data);
      }
    }
    
    if (!db instanceof Map) {
      // Make sure all elements are written to file
      await db.saveToDisk();
    }
  }

