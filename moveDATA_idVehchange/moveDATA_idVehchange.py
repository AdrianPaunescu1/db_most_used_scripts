import mysql.connector
from mysql.connector import Error
from warnings import filterwarnings
import logging
from dotenv import load_dotenv
import os

load_dotenv()

# LOGGING FILE CONFIG, TO READ ALL DETAILS INTO OUTSIDE FILE!
logging.basicConfig(
    filename='log_IdVehChange.log',
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    # datefmt='%H:%M:%S'[%(asctime)s]
)

try:
    connection = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        auth_plugin='mysql_native_password'
    )
    
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        logging.info("Connected to MySQL Server version: " + db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        logging.info("You're connected to database: " + record[0])

        # ASK FOR VEHICLES ID’S
        print('Enter the ID of the vehicle from which you want to move data:')
        OldIdVeh = input()
        logging.info("The old vehicle IdVeh entered is: " + OldIdVeh)
        print("Enter the ID of the vehicle to which you want to move the data:")
        NewIdVeh = input()
        logging.info("The new vehicle IdVeh entered is: " + NewIdVeh)

        # CHECK IF ID’S ARE IN NUMERIC FORMAT
        if(OldIdVeh.isdigit() == False or NewIdVeh.isdigit() == False):
            print(
                "Incorrect ID format, they must be numeric. Please try again!")

        # Query, cursor and Values to update, prepare for running!
        # Update sat_msgPos with values from input()
        query1 = "UPDATE IGNORE sat_msgPos SET IdVeh = %s WHERE IdVeh = %s"
        # Update sat_vpr with same values
        query2 = "UPDATE IGNORE sat_vpr SET IdVeh = %s WHERE IdVeh = %s "
        # Update sat_vudLeg with same values
        query3 = "UPDATE IGNORE sat_vudLeg SET IdVeh = %s WHERE IdVeh = %s "
        cursor = connection.cursor(prepared=True)
        ValuesToUpdate = (NewIdVeh, OldIdVeh)

        # Run query to update sat_msgPos
        cursor.execute(query1, ValuesToUpdate)
        connection.commit()
        print('Execution changed ' + str(cursor.rowcount) + ' rows')
        logging.info('Execution changed ' + str(cursor.rowcount) + ' rows')
        connection.get_warnings = True  # Warnings Fetch and Show
        msg = cursor.fetchwarnings()
        if msg is not None:
            for m in msg:
                logging.warning("{msg}".format(msg=msg[2]))
        else:
            print("Update of data in sat_msgPos table completed successfully!")
            logging.info(
                'Update of data in sat_msgPos table completed successfully!')

        # Run query to update sat_vpr
        cursor.execute(query2, ValuesToUpdate)
        connection.commit()
        print('Execution changed ' + str(cursor.rowcount) + ' rows')
        logging.info('Execution changed ' + str(cursor.rowcount) + ' rows')
        connection.get_warnings = True  # Warnings Fetch and Show
        msg = cursor.fetchwarnings()
        if msg is not None:
            for m in msg:
                logging.warning("{msg}".format(msg=msg[2]))
        else:
            print("Update of data in sat_vpr table completed successfully!")
            logging.info(
                'Update of data in sat_vpr table completed successfully!')

        # Run query to update sat_vudLeg
        cursor.execute(query3, ValuesToUpdate)
        connection.commit()
        print('Execution changed ' + str(cursor.rowcount) + ' rows')
        logging.info('Execution changed ' + str(cursor.rowcount) + ' rows')
        connection.get_warnings = True  # Warnings Fetch and Show
        msg = cursor.fetchwarnings()
        if msg is not None:
            for m in msg:
                logging.warning("{msg}".format(msg=msg[2]))
        else:
            print("Update of data in sat_vudLeg table completed successfully!")
            logging.info(
                'Update of data in sat_vudLeg table completed successfully!')

except Error as e:
    print("Error while connecting to MySQL", e)
    logging.CRITICAL("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("Connection to MySQL has been successfully closed!")
        logging.info("Connection to MySQL has been successfully closed!")
        logging.info("--------------------End-----------------------")
    else:
        logging.CRITICAL("Error while connecting to MySQL!")
