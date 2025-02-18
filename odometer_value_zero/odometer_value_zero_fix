import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
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
    if connection.is_connected():
        cursor = connection.cursor()

        # Select the IDs of rows where amDistTot=0
        select_query = "SELECT idMsg FROM sat_msgPos WHERE amDistTot = 0 and idVeh=30431 and tmMsg between 20230601 and 20230624 and idTrigger=3"
        cursor.execute(select_query)

        rows = cursor.fetchall()

        # Iterate through the results and update the amDistTot values
        for row in rows:
            id_msg = row[0]

            # Select the previous non-zero value
            prev_query = "SELECT amDistTot FROM sat_msgPos WHERE idMsg < %s AND amDistTot <> 0 and idVeh=30431 and tmMsg between 20230601 and 20230624 ORDER BY idMsg DESC LIMIT 1"
            cursor.execute(prev_query, (id_msg,))
            prev_row = cursor.fetchone()

            if prev_row is not None:
                prev_amDistTot = prev_row[0]

                # Update the amDistTot value with the previous non-zero value
                update_query = "UPDATE sat_msgPos SET amDistTot = %s WHERE idMsg = %s and idVeh=30431 and tmMsg between 20230601 and 20230624"
                cursor.execute(update_query, (prev_amDistTot, id_msg))
                connection.commit()

        print("The 'amDistTot' values have been successfully updated for rows where 'amDistTot' = 0.")

except mysql.connector.Error as error:
    print("Error while connecting to MySQL", error)

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
