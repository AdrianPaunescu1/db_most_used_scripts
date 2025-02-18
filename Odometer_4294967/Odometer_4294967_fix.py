import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

try:
    # Connect to MySQL using environment variables
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

        # Select erroneous messages with idTrigger in (3, 4) and amDistTot = 4294967
        query = """
            SELECT idMsg, amDistTot 
            FROM sat_msgPos 
            WHERE idVeh = 15687 
            AND tmMsg BETWEEN 20240901 AND 20240931 
            AND amDistTot = 4294967
            AND idTrigger IN (3, 4)
            ORDER BY tmMsg
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        updated_rows = []

        print(f"Total erroneous messages: {len(rows)}")

        # Check each erroneous message and look for a valid previous message
        for i in range(len(rows)):
            current_id_msg, current_amDistTot = rows[i]
            
            # Look for a valid previous message (idTrigger NOT IN (3, 4) and amDistTot != 4294967)
            prev_query = """
                SELECT idMsg, amDistTot 
                FROM sat_msgPos 
                WHERE idVeh = 15687 
                AND idMsg < %s 
                AND idTrigger NOT IN (3, 4)
                AND amDistTot != 4294967
                AND tmMsg BETWEEN 20240901 AND 20240931 
                ORDER BY idMsg DESC
                LIMIT 1
            """
            cursor.execute(prev_query, (current_id_msg,))
            prev_result = cursor.fetchone()

            if prev_result:
                prev_id_msg, prev_amDistTot = prev_result
                updated_rows.append((current_id_msg, prev_amDistTot))

                # Update amDistTot value in the database
                update_query = """
                    UPDATE sat_msgPos 
                    SET amDistTot = %s 
                    WHERE idMsg = %s
                """
                cursor.execute(update_query, (prev_amDistTot, current_id_msg))
                connection.commit()
                print(f"ID {current_id_msg} has been updated with amDistTot = {prev_amDistTot}")
            else:
                print(f"No valid previous message found for ID {current_id_msg}")

        # Display the number of updated rows
        if updated_rows:
            print(f"Number of corrected messages: {len(updated_rows)}")
        else:
            print("No messages found to correct.")

except mysql.connector.Error as error:
    print("Error while connecting to MySQL", error)

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
