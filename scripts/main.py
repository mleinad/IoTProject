import os
from dotenv import load_dotenv
from Database.mysql_connector import connect_mysql
from Database.create_tables import create_denormalized_table, get_table_info, drop_all_tables
from Database.extract_data import import_both_datasets
from Database.queries import *

def main():
    # Load environment variables
    load_dotenv(dotenv_path=".env")
    MYSQL_CONNECTOR_HOST = os.getenv("MYSQL_DB_HOST")
    MYSQL_CONNECTOR_USER = os.getenv("MYSQL_DB_USER")
    MYSQL_CONNECTOR_PASSWORD = os.getenv("MYSQL_DB_PASSWORD")

    mysql_database = "IoT_project"

    MYSQL_CONNECTOR = [MYSQL_CONNECTOR_HOST, 
                       MYSQL_CONNECTOR_USER, 
                       MYSQL_CONNECTOR_PASSWORD]


    # Create database if it doesn't exist
    sqlConnection = connect_mysql(MYSQL_CONNECTOR, "")
    mycursor = sqlConnection.cursor()
    mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_database}")
    print(f"Database '{mysql_database}' ready\n")
    sqlConnection.close()

    # Connect to the database
    sqlConnection = connect_mysql(MYSQL_CONNECTOR, mysql_database)
    if sqlConnection is None:
        print(f"Failed to connect to database '{mysql_database}'")
        return
    
#    drop_all_tables(sqlConnection) #drop tables to apply changes

    # Create tables
    if not create_denormalized_table(sqlConnection):
        print("Failed to create tables")
        sqlConnection.close()
        return

    # Import CSV data
#    print(f"Starting CSV import from: {csv_file_path}")
    if import_both_datasets(sqlConnection):
        print("CSV data imported successfully!\n")
    else:
        print("CSV import failed!")

    # Show statistics
#    get_table_info(sqlConnection)

    # QUERIES
    # Run any query you want
    results = get_power_distribution(sqlConnection)
    for row in results[:5]:
        print(row)
    sqlConnection.close()
    print("\nDatabase connection closed")
    print("\nNow run: streamlit run streamlit_dashboard.py")

if __name__ == "__main__":
    main()
