from mysql.connector import Error


def create_denormalized_table(connection):
    """
    Create a single denormalized table for EV charging data.
    """
    try:
        cursor = connection.cursor()
        
        print("Creating database tables...")
        
        # Main EV charging data table
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
        print("✓ EV charging data table created")
        
        # Stations table with location data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ev_stations (
                station_id VARCHAR(50) PRIMARY KEY,
                distrito VARCHAR(100),
                concelho VARCHAR(100),
                freguesia VARCHAR(100),
                latitude DECIMAL(10,7),
                longitude DECIMAL(10,7),
                potencia_maxima_kw DECIMAL(8,2),
                pontos_ligacao INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_distrito (distrito),
                INDEX idx_concelho (concelho),
                INDEX idx_location (latitude, longitude)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        print("✓ EV stations table created")
        
        connection.commit()
        cursor.close()
        print("\nTables created successfully!\n")
        return True
        
    except Error as e:
        print(f"Error creating tables: {e}")
        return False


def drop_all_tables(connection):
    """
    Drop all tables (useful for testing/resetting database).
    """
    try:
        cursor = connection.cursor()
        
        print("Dropping all tables...")
        
        cursor.execute("DROP TABLE IF EXISTS ev_charging_data")
        cursor.execute("DROP TABLE IF EXISTS ev_stations")
        
        connection.commit()
        cursor.close()
        print("All tables dropped successfully!\n")
        return True
        
    except Error as e:
        print(f"Error dropping tables: {e}")
        return False


def get_table_info(connection):
    """
    Display information about all tables in the database.
    """
    try:
        cursor = connection.cursor()
        
        print("\n=== Database Statistics ===")
        
        # Charging data stats
        cursor.execute("SELECT COUNT(*) FROM ev_charging_data")
        count = cursor.fetchone()[0]
        print(f"Total charging sessions: {count}")
        
        if count > 0:
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM ev_charging_data")
            users = cursor.fetchone()[0]
            print(f"Unique users: {users}")
            
            cursor.execute("SELECT COUNT(DISTINCT charging_station_id) FROM ev_charging_data")
            stations_used = cursor.fetchone()[0]
            print(f"Charging stations used: {stations_used}")
            
            cursor.execute("SELECT COUNT(DISTINCT vehicle_model) FROM ev_charging_data")
            models = cursor.fetchone()[0]
            print(f"Unique vehicle models: {models}")
        
        # Stations stats
        cursor.execute("SELECT COUNT(*) FROM ev_stations")
        stations = cursor.fetchone()[0]
        print(f"\nTotal stations in database: {stations}")
        
        if stations > 0:
            cursor.execute("SELECT COUNT(DISTINCT distrito) FROM ev_stations")
            distritos = cursor.fetchone()[0]
            print(f"Distritos covered: {distritos}")
            
            cursor.execute("SELECT COUNT(DISTINCT concelho) FROM ev_stations")
            concelhos = cursor.fetchone()[0]
            print(f"Concelhos covered: {concelhos}")
        
        print("===========================\n")
        
        cursor.close()
        
    except Error as e:
        print(f"Error getting table info: {e}")
