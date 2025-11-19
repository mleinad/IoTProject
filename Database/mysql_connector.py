import mysql.connector

def connect_mysql(MYSQLCONNECTOR, database):
    conn = None
    try:
        conn = mysql.connector.connect(
            host=MYSQLCONNECTOR[0],
            user=MYSQLCONNECTOR[1],
            password=MYSQLCONNECTOR[2],
            database=database
        )
        if conn.is_connected():
            print(f"Connected to MySQL database '{database}'")
    except mysql.connector.Error as e:
        print(f"Error: {e}")
    return conn

