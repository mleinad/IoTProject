import csv
from datetime import datetime
from mysql.connector import Error

def parse_csv_datetime(date_str):
    """
    Parse datetime from DD/MM/YY HH:MM format.
    
    Args:
        date_str: Date string in DD/MM/YY HH:MM format
    
    Returns:
        datetime object or None if parsing fails
    """
    try:
        return datetime.strptime(date_str, '%d/%m/%y %H:%M')
    except ValueError:
        print(f"Error parsing date: {date_str}")
        return None


def parse_decimal(value_str):
    """
    Convert comma decimal separator to dot and handle empty values.
    
    Args:
        value_str: String with comma as decimal separator
    
    Returns:
        String with dot as decimal separator, or None for empty values
    """
    if not value_str or value_str.strip() == '':
        return None
    return value_str.replace(',', '.')


def parse_int(value_str):
    """
    Parse integer value and handle empty strings.
    
    Args:
        value_str: String representation of integer
    
    Returns:
        Integer value or None for empty values
    """
    if not value_str or value_str.strip() == '':
        return None
    try:
        return int(value_str)
    except ValueError:
        return None


def extract_ev_charging_data_normalized(csv_file_path, connection, batch_size=1000):
    """
    Extract EV charging data from CSV and insert into normalized database structure.
    Uses batch inserts for improved performance.
    
    Args:
        csv_file_path: Path to the CSV file
        connection: MySQL connection object
        batch_size: Number of rows to insert in each batch (default: 1000)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = connection.cursor()
        
        # Track inserted entities to avoid redundant queries
        users_set = set()
        vehicles_dict = {}  # Key: (user_id, vehicle_model), Value: vehicle_id
        stations_set = set()
        session_count = 0
        skipped_rows = 0
        
        # Batch storage for charging sessions
        session_batch = []
        
        # Use utf-8-sig to handle BOM if present
        with open(csv_file_path, 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            
            # Strip whitespace from column names
            csv_reader.fieldnames = [name.strip() for name in csv_reader.fieldnames]
            
            # Debug: Print column names to verify
            print(f"CSV Columns found: {csv_reader.fieldnames}\n")
            
            for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 (after header)
                try:
                    # Strip whitespace from all values
                    row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
                    
                    user_id = row['User ID']
                    station_id = row['Charging Station ID']
                    vehicle_model = row['Vehicle Model']
                    
                    # Skip rows with missing critical data
                    if not user_id or not station_id or not vehicle_model:
                        print(f"Skipping row {row_num}: Missing critical data")
                        skipped_rows += 1
                        continue
                    
                    # Insert user if not exists
                    if user_id not in users_set:
                        cursor.execute(
                            "INSERT IGNORE INTO users (user_id) VALUES (%s)",
                            (user_id,)
                        )
                        users_set.add(user_id)
                    
                    # Insert charging station if not exists
                    if station_id not in stations_set:
                        cursor.execute(
                            "INSERT IGNORE INTO charging_stations (station_id) VALUES (%s)",
                            (station_id,)
                        )
                        stations_set.add(station_id)
                    
                    # Insert or get vehicle
                    vehicle_key = (user_id, vehicle_model)
                    if vehicle_key not in vehicles_dict:
                        cursor.execute(
                            """INSERT INTO vehicles 
                               (user_id, vehicle_model, battery_capacity_kwh, vehicle_age_years) 
                               VALUES (%s, %s, %s, %s)""",
                            (user_id, 
                             vehicle_model,
                             parse_decimal(row['Battery Capacity (kWh)']),
                             parse_int(row['Vehicle Age (years)']))
                        )
                        vehicles_dict[vehicle_key] = cursor.lastrowid
                    
                    vehicle_id = vehicles_dict[vehicle_key]
                    
                    # Parse datetime fields
                    start_time = parse_csv_datetime(row['Charging Start Time'])
                    end_time = parse_csv_datetime(row['Charging End Time'])
                    
                    if start_time is None or end_time is None:
                        print(f"Skipping row {row_num} due to invalid datetime")
                        skipped_rows += 1
                        continue
                    
                    # Add to batch instead of immediate insert
                    # Use parse_decimal for all numeric fields (returns None for empty strings)
                    session_data = (
                        user_id, 
                        vehicle_id, 
                        station_id,
                        start_time,
                        end_time,
                        parse_decimal(row['Energy Consumed (kWh)']),
                        parse_decimal(row['Charging Duration (hours)']),
                        parse_decimal(row['Charging Rate (kW)']),
                        parse_decimal(row['Charging Cost (EUR)']),
                        row['Time of Day'] if row['Time of Day'] else None,
                        row['Day of Week'] if row['Day of Week'] else None,
                        parse_decimal(row['State of Charge (Start %)']),
                        parse_decimal(row['State of Charge (End %)']),
                        parse_decimal(row['Distance Driven (since last charge) (km)']),
                        parse_decimal(row['Temperature (C)'])
                    )
                    
                    session_batch.append(session_data)
                    
                    # Execute batch insert when batch_size is reached
                    if len(session_batch) >= batch_size:
                        _insert_session_batch(cursor, session_batch)
                        session_count += len(session_batch)
                        print(f"Inserted batch: {session_count} sessions processed")
                        session_batch = []
                        
                except KeyError as e:
                    print(f"Row {row_num}: Missing column {e}")
                    print(f"Available columns: {list(row.keys())}")
                    skipped_rows += 1
                    continue
                except Exception as e:
                    print(f"Row {row_num}: Error processing row: {e}")
                    skipped_rows += 1
                    continue
            
            # Insert remaining sessions in batch
            if session_batch:
                _insert_session_batch(cursor, session_batch)
                session_count += len(session_batch)
            
            connection.commit()
            
            print(f"\n=== Import Summary ===")
            print(f"Charging sessions: {session_count}")
            print(f"Skipped rows: {skipped_rows}")
            print(f"Unique users: {len(users_set)}")
            print(f"Unique vehicles: {len(vehicles_dict)}")
            print(f"Unique stations: {len(stations_set)}")
            print(f"======================\n")
            
            cursor.close()
            return True
            
    except Error as e:
        print(f"MySQL Error importing data: {e}")
        connection.rollback()
        return False
    except Exception as e:
        print(f"Error importing data: {e}")
        connection.rollback()
        return False


def _insert_session_batch(cursor, batch_data):
    """
    Internal helper function to perform batch insert of charging sessions.
    
    Args:
        cursor: MySQL cursor object
        batch_data: List of tuples containing session data
    """
    sql = """INSERT INTO charging_sessions 
             (user_id, vehicle_id, station_id, charging_start_time, 
              charging_end_time, energy_consumed_kwh, charging_duration_hours,
              charging_rate_kw, charging_cost_eur, time_of_day, day_of_week,
              state_of_charge_start_pct, state_of_charge_end_pct, 
              distance_driven_km, temperature_c)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    cursor.executemany(sql, batch_data)


def extract_with_progress(csv_file_path, connection, batch_size=1000, show_progress=True):
    """
    Extract data with progress reporting (useful for large files).
    
    Args:
        csv_file_path: Path to the CSV file
        connection: MySQL connection object
        batch_size: Number of rows to insert in each batch
        show_progress: Whether to show progress updates
    
    Returns:
        bool: True if successful, False otherwise
    """
    if show_progress:
        # Count total rows first (optional, for progress percentage)
        with open(csv_file_path, 'r', encoding='utf-8-sig') as f:
            total_rows = sum(1 for _ in f) - 1  # Subtract header
        print(f"Total rows to process: {total_rows}")
    
    return extract_ev_charging_data_normalized(csv_file_path, connection, batch_size)
