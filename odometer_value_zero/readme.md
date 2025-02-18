# GPS Data Correction Script

This Python script connects to a MySQL database to update GPS data. It specifically addresses rows where the value of `amDistTot` is 0 and updates them with the last valid non-zero value from previous rows.

## How it works:
1. The script selects the `idMsg` for rows where `amDistTot` is 0 and within a certain date range (`tmMsg between 20230601 and 20230624`).
2. For each selected row, it retrieves the last non-zero `amDistTot` value before the current row.
3. The script then updates the row with the correct value.
