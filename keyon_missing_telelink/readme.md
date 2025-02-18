# GPS Data Script for MySQL

This script connects to a MySQL database, iterates over a specific date range, and updates the `idTrigger` field for specific messages related to a specific vehicle.
It checks for messages where the engine is off (`idTrigger=4`) and, if there are no messages with engine on in that specific day, updates the idTrigger of the first message from that day to KEYON('idTrigger=3').
