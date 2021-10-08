const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const util = require('util');
const csv = require('fast-csv');
const N3 = require('n3');
const Store = require('./stores/Store');
const Services = require('./services/calendar');
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

    var stops = fs.createReadStream(path + '/stops.txt', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true }))
        .on('error', function (e) {
            console.error(e);
        });

    var routes = fs.createReadStream(path + '/routes.txt', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true }))
        .on('error', function (e) {
            console.error(e);
        });

    var trips = fs.createReadStream(path + '/trips.txt', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true }))
        .on('error', function (e) {
            console.error(e);
        });

    var calendarDates = fs.createReadStream(path + '/calendar_dates.txt', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true }))
        .on('error', function (e) {
            console.error(e);
        });

    var services = fs.createReadStream(path + '/calendar.txt', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true }))
        .pipe(new Services(calendarDates, this._options))
        .on('error', function (e) {
            console.error(e);
        });

    var options = this._options;
    var stopsdb = Store(output + '/.stops', options.store);
    var routesdb = Store(output + '/.routes', options.store);
    var tripsdb = Store(output + '/.trips', options.store);
    var servicesdb = Store(output + '/.services', options.store);
    var count = 0;

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
                            await exec(`cat raw_* > linkedConnections.${ext}`, { cwd: output });
                            let t1 = new Date();
                            console.error('linkedConnections.' + ext + ' File created in ' + (t1.getTime() - t0.getTime()) + ' ms');
                            await del([
                                path + '/connections_*',
                                path + '/trips_*',
                                output + '/.stops',
                                output + '/.routes',
                                output + '/.trips',
                                output + '/.services',
                                output + '/raw_*'
                            ],
                                { force: true });
                            done(`${output}/linkedConnections.${ext}`);
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
    createServiceIndex(services, servicesdb).then(() => finished());
    createIndex('route_id', routes, routesdb).then(() => finished());
    createIndex('trip_id', trips, tripsdb).then(() => finished());
    createIndex('stop_id', stops, stopsdb).then(() => finished());

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

    async function createServiceIndex(stream, db) {
        for await (const data of stream) {
            if (data['service_id']) {
                await db.set(data['service_id'], data['dates']);
            }
        }
    }

    // Code executed only on a Worker Thread
    if (!isMainThread) {
        // Read the connections file created by the gtfs2lc-sort script
        var connectionRules = fs.createReadStream(workerData['path'] + '/connections_' + workerData['instance'] + '.txt', { encoding: 'utf8', objectMode: true })
            .pipe(csv.parse({ objectMode: true, headers: true }))
            .on('error', function (e) {
                console.error('Hint: Did you run gtfs2lc-sort?');
                console.error(e);
            });

        let routesdb = null;
        let tripsdb = null;
        let servicesdb = null;
        let stopsdb = null;

        if (workerData['options']['store'] === 'KeyvStore') {
            // Rebuild the KeyvStore objects
            routesdb = Store(workerData['routesdb'], 'KeyvStore');
            tripsdb = Store(workerData['tripsdb'], 'KeyvStore');
            servicesdb = Store(workerData['servicesdb'], 'KeyvStore');
            stopsdb = Store(workerData['stopsdb'], 'KeyvStore');
        } else {
            routesdb = workerData['routesdb'];
            tripsdb = workerData['tripsdb'];
            servicesdb = workerData['servicesdb'];
            stopsdb = workerData['stopsdb'];
        }
    }

    // Build the StopTimes objects !
};