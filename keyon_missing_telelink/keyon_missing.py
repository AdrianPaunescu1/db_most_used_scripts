import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

try:
    # Use environment variables for database connection
    connection = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        auth_plugin='mysql_native_password'
    )
    cursor = connection.cursor()

    # Define the period to scan the days
    start_date = datetime(2024, 3, 1)  # Start date of the month
    end_date = datetime(2024, 4, 1)    # End date of the month

    # Iterate through each day in the given range
    current_date = start_date
    while current_date <= end_date:
        # Extract the current date as a string
        current_date_str = current_date.strftime('%Y-%m-%d')

        # Query to select the first position on the current day with idTrigger 4 for vehicle with idVeh=26979
        check_query = """
            SELECT idMsg, idTrigger
            FROM sat_msgPos
            WHERE DATE(tmMsg) = %s AND idVeh = 26979 AND idTrigger in (3,4)
            ORDER BY tmMsg ASC
            LIMIT 1
        """
        cursor.execute(check_query, (current_date_str,))
        message = cursor.fetchone()

        # If a message exists for the current day and has idTrigger 4
        if message:
            print(f"On {current_date_str}, the first position starts with idTrigger 4 (engine off).")

            # Query to select the first position for the current day with idTrigger not 3 or 4
            update_query = """
                SELECT idMsg
                FROM sat_msgPos
                WHERE DATE(tmMsg) = %s AND idVeh = 26979 AND idTrigger NOT IN (3,4)
                ORDER BY tmMsg ASC
                LIMIT 1
            """
            cursor.execute(update_query, (current_date_str,))
            message_to_update = cursor.fetchone()

            # If such a message exists, update the idTrigger to 3
            if message_to_update:
                update_msg_query = """
                    UPDATE sat_msgPos
                    SET idTrigger = 3
                    WHERE idMsg = %s
                """
                cursor.execute(update_msg_query, (message_to_update[0],))
                connection.commit()  # Save the changes in the database

        # Move to the next day
        current_date += timedelta(days=1)

    # Close the database connection
    cursor.close()
    connection.close()

except mysql.connector.Error as error:
    print("Error connecting to the database:", error)
