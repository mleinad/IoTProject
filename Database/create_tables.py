import mysql.connector


def create_denormalized_table(connection):
    try:
        cursor = connection.cursor()
                
        # Single denormalized table with all EV charging data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ev_charging_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(50),
                vehicle_model VARCHAR(100),
                battery_capacity_kwh DECIMAL(5,2),
                charging_station_id VARCHAR(50),
                charging_start_time DATETIME,
                charging_end_time DATETIME,
                energy_consumed_kwh DECIMAL(6,2),
                charging_duration_hours DECIMAL(5,3),
                charging_rate_kw DECIMAL(6,2),
                charging_cost_eur DECIMAL(8,2),
                time_of_day VARCHAR(20),
                day_of_week VARCHAR(20),
                state_of_charge_start_pct DECIMAL(5,2),
                state_of_charge_end_pct DECIMAL(5,2),
                distance_driven_km DECIMAL(7,2),
                temperature_c DECIMAL(4,2),
                vehicle_age_years INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                INDEX idx_station_id (charging_station_id),
                INDEX idx_start_time (charging_start_time),
                INDEX idx_day_week (day_of_week),
                INDEX idx_time_of_day (time_of_day)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        connection.commit()
        cursor.close()
        print("\nTable created successfully!\n")
        return True
        
    except mysql.connector.Error as e:
        print(f"Error creating table: {e}")
        return False


def drop_all_tables(connection):
    try:
        cursor = connection.cursor()
                
        cursor.execute("DROP TABLE IF EXISTS ev_charging_data")
        
        connection.commit()
        cursor.close()
        print("Table dropped successfully!\n")
        return True
        
    except mysql.connector.Error as e:
        print(f"Error dropping table: {e}")
        return False


def get_table_info(connection):
    try:
        cursor = connection.cursor()
        
        print("\n=== Database Statistics ===")
        cursor.execute("SELECT COUNT(*) FROM ev_charging_data")
        count = cursor.fetchone()[0]
        print(f"Total charging sessions: {count}")
        
        # Additional statistics
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM ev_charging_data")
        users = cursor.fetchone()[0]
        print(f"Unique users: {users}")
        
        cursor.execute("SELECT COUNT(DISTINCT charging_station_id) FROM ev_charging_data")
        stations = cursor.fetchone()[0]
        print(f"Unique charging stations: {stations}")
        
        cursor.execute("SELECT COUNT(DISTINCT vehicle_model) FROM ev_charging_data")
        models = cursor.fetchone()[0]
        print(f"Unique vehicle models: {models}")
        
        print("===========================\n")
        
        cursor.close()
        
    except mysql.connector.Error as e:
        print(f"Error getting table info: {e}")
        return False