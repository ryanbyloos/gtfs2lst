const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const util = require('util');
const csv = require('fast-csv');
const N3 = require('n3');
const Store = require('./stores/Store');
const Services = require('./services/calendar');
const StoptimesBuilder = require('./StoptimesBuilder')
const jsonldstream = require('jsonld-stream');
const cp = require('child_process');
const del = require('del');
const numCPUs = require('os').cpus().length;

const readdir = util.promisify(fs.readdir);
const appendFile = util.promisify(fs.appendFile);
const exec = util.promisify(cp.exec);

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
    var count = 0;

    // Step 3: Create connections in parallel using worker threads
    var finished = function () {
        count++;
        // Wait for the 4 streams to finish (services, routes and stops) to write to the stores
        if (count === 4) {
            console.error("Indexing of stops, services, routes and trips completed successfully!");
            let w = 0;
            // Create as many worker threads as there are available CPUs
            for (let i = 0; i < numCPUs; i++) {
                const worker = new Worker(__filename, {
                    workerData: {
                        instance: i,
                        path: path,
                        output: output,
                        routesdb: options.store === 'MemStore' ? routesdb : output + '/.routes',
                        tripsdb: options.store === 'MemStore' ? tripsdb : output + '/.trips',
                        servicesdb: options.store === 'MemStore' ? servicesdb : output + '/.services',
                        stopsdb: options.store === 'MemStore' ? stopsdb : output + '/.stops',
                        options: options
                    }
                });

                console.error(`Created worker thread (PID ${worker.threadId})`);

                worker.on('message', async () => {
                    w++;
                    if (w === numCPUs) {
                        // Merge all the created files into one
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
                    }
                }).on('error', err => {
                    console.error(err);
                }).on('exit', (code) => {
                    if (code !== 0) {
                        console.error(new Error(`Worker stopped with exit code ${code}`));
                    }
                });
            }
        }
    };

    // Create GTFS indexes
    createIndex('trip_id', stoptimes, stoptimesdb).then(() => finished());

    var appendLineBreaks = async () => {
        let files = (await readdir(output)).filter(raw => raw.startsWith('raw_'));
        for (const [i, f] of files.entries()) {
            if (i < files.length - 1) {
                await appendFile(`${output}/${f}`, '\n')
            }
        }
    };

    var removePrefixes = async () => {
        let files = (await readdir(output)).filter(raw => raw.startsWith('raw_'));
        for (const [i, f] of files.entries()) {
            if (i > 0) {
                // TODO: find a not hard-coded way to remove prefixes
                await exec(`sed -i 1,4d ${f}`, { cwd: output });
            }
        }
    };
};

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

// Code executed only on a Worker Thread
if (!isMainThread) {
    // Read the connections file created by the gtfs2lc-sort script
    var stopTimesRules = fs.createReadStream(path + '/stoptimes.txt', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true }))
        .on('error', error => console.error(error));

    let stoptimesdb = null;

    if (workerData['options']['store'] === 'KeyvStore') {
        // Rebuild the KeyvStore objects
        stoptimesdb = Store(workerData['stoptimesdb'], 'KeyvStore');
    } else {
        stoptimesdb = workerData['stoptimesdb'];
    }

    // TODO: Build the Stoptimes object

    // TODO: proceed to parse the stoptimes according to the requested format
    let format = workerData['options']['format'];

    if (!format || ['json', 'mongo'].indexOf(format) >= 0) {
        if (format === 'mongo') {
            // connectionStream = connectionStream.pipe(new Connections2Mongo());
        }

        connectionStream = connectionStream.pipe(new jsonldstream.Serializer())
            .pipe(fs.createWriteStream(workerData['output'] + '/raw_' + workerData['instance'] + '.json'));
    } else if (['jsonld', 'mongold'].indexOf(format) >= 0) {
        let context = undefined;
        // TODO: create the context for the first instance

    }
}

module.exports = Mapper;
