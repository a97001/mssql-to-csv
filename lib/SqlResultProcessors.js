module.exports = {
    tableForm(csvStream, recordset, now) {
        for (const record of recordset) {
            record.timestamp = new Date(record.timestamp).toISOString();
            console.log(record);
            csvStream.write(record);
        }
    },

    pivotForm(csvStream, recordset, now) {
        const resultRecord = { timestamp: now.toISOString() };
        for (const record of recordset) {
            resultRecord[record.header] = record.value;
        }
        csvStream.write(resultRecord);
    }
};
