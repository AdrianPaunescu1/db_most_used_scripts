# Vehicle Data Update Script

This Python script allows you to change the vehicle ID in three different tables (`sat_msgPos`, `sat_vpr`, `sat_vudLeg`) in a MySQL database. The script allows you to move data from one vehicle to another by updating the `IdVeh` field based on user input.

It is designed to connect to a MySQL database, run update queries, and log details and errors in a log file.
