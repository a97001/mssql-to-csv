const config = require(`${process.cwd()}/config.json`);
const csv = require('fast-csv');
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const SqlResultProcessor = require('./lib/SqlResultProcessors');

(async () => {
    const now = Date.now();
    const pool = new sql.ConnectionPool({
        server: config.connection.server,
        database: config.connection.database,
        user: config.connection.user,
        password: config.connection.password
        // options: {
        //     trustedConnection: 'yes'
        // }
        // connectionString: 'Driver={SQL Server Native Client 11.0};Server={COMON-PC};Database={test};Trusted_Connection={yes};'
    });

    await pool.connect();

    const request = new sql.Request(pool);
    // request.input('timestamp', sql.TYPES.DateTime, now);
    const result = await request.query(config.query);

    // console.dir(result);

    if (result && result.recordsets) {
        let pendingCSVCount = result.recordsets.length;
        let recordsetIdx = 0;
        for (const recordset of result.recordsets) {
            const csvName = `${config.csvName}_${recordsetIdx}_${new Date().valueOf()}.csv`;
            const csvStream = csv.createWriteStream({ headers: true });
            const writableStream = fs.createWriteStream(path.join(config.csvExportDir, csvName));
            writableStream.on('finish', () => { // eslint-disable-line no-loop-func
                console.log('DONE!');
                pendingCSVCount--;
                if (pendingCSVCount === 0) {
                    process.exit();
                }
            });
            csvStream.pipe(writableStream);
            SqlResultProcessor[config.sqlResultProcessor](csvStream, recordset, now);
            csvStream.end();
            recordsetIdx++;
        }
    }
})();
