import time
import mysql.connector

def connect_mysql(connector_info, database):
    host, user, password = connector_info
    for attempt in range(10):  # try up to 10 times
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            print("Connected to MySQL!")
            return conn
        except mysql.connector.Error:
            print(f"MySQL not ready, retrying in 3s... ({attempt+1}/10)")
            time.sleep(3)
    return None
