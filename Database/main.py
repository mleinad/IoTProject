import os
from dotenv import load_dotenv
from data_sql.mysql_connector import connect_mysql
from data_sql.create_tables import create_denormalized_table, get_table_info, drop_all_tables
from data_sql.extract_data import import_both_datasets
from data_sql.queries import *


def main():
    # Load environment variables
    load_dotenv(dotenv_path="../.env")
    MYSQL_CONNECTOR_HOST = os.getenv("MYSQL_DB_HOST")
    MYSQL_CONNECTOR_USER = os.getenv("MYSQL_DB_USER")
    MYSQL_CONNECTOR_PASSWORD = os.getenv("MYSQL_DB_PASSWORD")

    mysql_database = "IoT_project"

    MYSQL_CONNECTOR = [MYSQL_CONNECTOR_HOST, 
                       MYSQL_CONNECTOR_USER, 
                       MYSQL_CONNECTOR_PASSWORD]

    # Create database if it doesn't exist (uncomment if needed)
    # sqlConnection = connect_mysql(MYSQL_CONNECTOR, "")
    # mycursor = sqlConnection.cursor()
    # mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_database}")
    # print(f"Database '{mysql_database}' ready\n")
    # sqlConnection.close()

    # Connect to the database
    sqlConnection = connect_mysql(MYSQL_CONNECTOR, mysql_database)
    if sqlConnection is None:
        print(f"Failed to connect to database '{mysql_database}'")
        return
    
    # Uncomment to drop and recreate tables
#    drop_all_tables(sqlConnection)

    # Create tables (includes new charging_sessions table)
    print("=" * 50)
    print("INITIALIZING DATABASE SCHEMA")
    print("=" * 50)
    
    if not create_denormalized_table(sqlConnection):
        print("Failed to create tables")
        sqlConnection.close()
        return

    # Import CSV data (historical data)
    # print("\n" + "=" * 50)
    # print("IMPORTING HISTORICAL DATA")
    # print("=" * 50)
    
    # if import_both_datasets(sqlConnection):
    #     print("✓ CSV data imported successfully!\n")
    # else:
    #     print("✗ CSV import failed!")

    # Show statistics
    get_table_info(sqlConnection)

    # Example queries (optional)
    print("=" * 50)
    print("RUNNING SAMPLE QUERIES")
    print("=" * 50)
    
    try:
        results = get_power_distribution(sqlConnection)
        print("\nSample query results:")
        for row in results[:5]:
            print(row)
    except Exception as e:
        print(f"Note: Sample query skipped ({e})")
    
    sqlConnection.close()
    print("\n" + "=" * 50)
    print("DATABASE INITIALIZATION COMPLETE")
    print("=" * 50)
    print("\nNext steps:")
    print("1. Start Docker services: docker-compose up -d")
    print("2. Run MQTT publisher: python Communication/mqtt_publisher.py")
    print("3. View dashboard: http://localhost:8501")
    print()


if __name__ == "__main__":
    main()
