import os
from dotenv import load_dotenv
from Database.mysql_connector import connect_mysql
from Database.create_tables import create_normalized_tables, get_table_info
from Database.extract_data import extract_ev_charging_data_normalized

def main():
    # Load environment variables
    load_dotenv(dotenv_path=".env")
    MYSQL_CONNECTOR_HOST = os.getenv("MYSQL_DB_HOST")
    MYSQL_CONNECTOR_USER = os.getenv("MYSQL_DB_USER")
    MYSQL_CONNECTOR_PASSWORD = os.getenv("MYSQL_DB_PASSWORD")

    mysql_database = "IoT_project"
    csv_file_path = "Data/dataset-EV_with_stations.csv"  # Update with your actual CSV file path

    MYSQL_CONNECTOR = [MYSQL_CONNECTOR_HOST, 
                       MYSQL_CONNECTOR_USER, 
                       MYSQL_CONNECTOR_PASSWORD]

    # Create database if it doesn't exist
    sqlConnection = connect_mysql(MYSQL_CONNECTOR, "")
    if sqlConnection is None:
        print("Failed to connect to MySQL server")
        return
    
    mycursor = sqlConnection.cursor()
    mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_database}")
    print(f"Database '{mysql_database}' ready\n")
    sqlConnection.close()

    # Connect to the database
    sqlConnection = connect_mysql(MYSQL_CONNECTOR, mysql_database)
    if sqlConnection is None:
        print(f"Failed to connect to database '{mysql_database}'")
        return

    # Create tables
    if not create_normalized_tables(sqlConnection):
        print("Failed to create tables")
        sqlConnection.close()
        return

    # Import CSV data
    print(f"Starting CSV import from: {csv_file_path}")
    if extract_ev_charging_data_normalized(csv_file_path, sqlConnection):
        print("CSV data imported successfully!\n")
        get_table_info(sqlConnection)
    else:
        print("CSV import failed!")

    sqlConnection.close()
    print("Database connection closed")

if __name__ == "__main__":
    main()
