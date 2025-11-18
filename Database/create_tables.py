from mysql.connector import Error

def create_normalized_tables(connection):
    """
    Create all normalized tables for the EV charging database.
    
    Args:
        connection: MySQL connection object
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = connection.cursor()
        
        print("Creating database tables...")
        
        # Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) PRIMARY KEY,
                registration_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        print("✓ Users table created")
        
        # Charging stations table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS charging_stations (
                station_id VARCHAR(50) PRIMARY KEY,
                location VARCHAR(255),
                max_charging_rate_kw DECIMAL(6,2),
                status ENUM('active', 'inactive', 'maintenance') DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        print("✓ Charging stations table created")
        
        # Vehicles table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vehicles (
                vehicle_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(50),
                vehicle_model VARCHAR(100),
                battery_capacity_kwh DECIMAL(5,2),
                vehicle_age_years INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        print("✓ Vehicles table created")
        
        # Charging sessions table (main fact table)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS charging_sessions (
                session_id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(50),
                vehicle_id INT,
                station_id VARCHAR(50),
                charging_start_time DATETIME,
                charging_end_time DATETIME,
                energy_consumed_kwh DECIMAL(6,2),
                charging_duration_hours DECIMAL(5,3),
                charging_rate_kw DECIMAL(6,2),
                charging_cost_eur DECIMAL(8,2),
                time_of_day ENUM('Morning', 'Afternoon', 'Evening', 'Night'),
                day_of_week ENUM('Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                                 'Friday', 'Saturday', 'Sunday'),
                state_of_charge_start_pct DECIMAL(5,2),
                state_of_charge_end_pct DECIMAL(5,2),
                distance_driven_km DECIMAL(7,2),
                temperature_c DECIMAL(4,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
                FOREIGN KEY (station_id) REFERENCES charging_stations(station_id) ON DELETE CASCADE,
                INDEX idx_user_id (user_id),
                INDEX idx_station_id (station_id),
                INDEX idx_start_time (charging_start_time),
                INDEX idx_day_week (day_of_week)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        print("✓ Charging sessions table created")
        
        connection.commit()
        cursor.close()
        print("\nAll tables created successfully!\n")
        return True
        
    except Error as e:
        print(f"Error creating tables: {e}")
        return False

def drop_all_tables(connection):
    """
    Drop all tables (useful for testing/resetting database).
    
    Args:
        connection: MySQL connection object
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = connection.cursor()
        
        print("Dropping all tables...")
        
        # Disable foreign key checks to allow dropping in any order
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        cursor.execute("DROP TABLE IF EXISTS charging_sessions")
        cursor.execute("DROP TABLE IF EXISTS vehicles")
        cursor.execute("DROP TABLE IF EXISTS charging_stations")
        cursor.execute("DROP TABLE IF EXISTS users")
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
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
    
    Args:
        connection: MySQL connection object
    """
    try:
        cursor = connection.cursor()
        
        tables = ['users', 'charging_stations', 'vehicles', 'charging_sessions']
        
        print("\n=== Database Statistics ===")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table.capitalize()}: {count} records")
        print("===========================\n")
        
        cursor.close()
        
    except Error as e:
        print(f"Error getting table info: {e}")
