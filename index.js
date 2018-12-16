const config = require(process.cwd()+'/config.json');
const csv = require('fast-csv');
const fs = require('fs');
const path = require('path');
const sql = require("mssql/msnodesqlv8");

(async () => {
    const now = Date.now();
    const pool = new sql.ConnectionPool({
        // server: "COMON-PC",
        // database: "test",
        // options: {
        //     trustedConnection: 'yes'
        // }
        connectionString: config.connectionString
    });

    await pool.connect();

    const request = new sql.Request(pool);
    request.input('timestamp', sql.TYPES.DateTime, now);
    const result = await request.query(config.query);

    console.dir(result);

    if (result && result.recordsets) {
        let pendingCSVCount = result.recordsets.length;
        let recordsetIdx = 0;
        for (const recordset of result.recordsets) {
            const csvName = `${config.csvName}_${recordsetIdx}_${new Date().valueOf()}.csv`;
            const csvStream = csv.createWriteStream({ headers: true });
            const writableStream = fs.createWriteStream(path.join(config.csvExportDir, csvName));
            writableStream.on("finish", function () {
                console.log("DONE!");
                pendingCSVCount--;
                if (pendingCSVCount === 0) {
                    process.exit();
                }
            });
            csvStream.pipe(writableStream);

            for (const record of recordset) {
                record.timestamp = new Date(record.timestamp).toISOString()
                console.log(record);
                csvStream.write(record);
            }
            csvStream.end();
            recordsetIdx++;
        }
    }
})();