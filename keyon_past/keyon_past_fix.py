import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

try:
    connection = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        auth_plugin=os.getenv('DB_AUTH_PLUGIN')
    )

    if connection.is_connected():
        cursor = connection.cursor()

        cursor.execute("""
           SELECT 
               m1.tmMsg AS tmMsg_current,
               (
                   SELECT m2.tmMsg
                   FROM sat_msgPos m2
                   WHERE m2.idVeh = m1.idVeh 
                   AND m2.tmMsg > m1.tmMsg 
                   AND m2.tmMsg > '2024-05-01' 
                   AND m2.idMsg > m1.idMsg 
                   AND m2.idSts=1
                   ORDER BY m2.tmMsg ASC
                   LIMIT 1
               ) AS tmMsg_next,
               m1.idMsg AS idMsg
           FROM 
               sat_msgPos m1
           WHERE 
               m1.idVeh in (30564)
               AND m1.amDistTot != 0 
               AND m1.idTrigger IN (3) 
               AND m1.tmMsg < '2022-01-01' 
               AND m1.tmUpdate > '2024-12-01';
        """)
        result = cursor.fetchall()

        for row in result:
            idMsg = row[2]  
            tmMsg_next = row[1]

            cursor.execute("UPDATE sat_msgPos SET tmMsg = %s WHERE idMsg = %s", (tmMsg_next, idMsg))
            connection.commit()
            print(f"Actualizare realizata pentru înregistrarea cu idMsg: {idMsg}")

except mysql.connector.Error as error:
    print("Eroare la conectare la MySQL:", error)

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
