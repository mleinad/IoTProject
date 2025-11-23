import csv
from datetime import datetime
import mysql.connector


def parse_csv_datetime(date_str):
    try:
        return datetime.strptime(date_str, '%d/%m/%y %H:%M')
    except ValueError:
        print(f"Error parsing date: {date_str}")
        return None


def parse_decimal(value_str):
    if not value_str or value_str.strip() == '':
        return None
    return value_str.replace(',', '.')


def parse_int(value_str):
    if not value_str or value_str.strip() == '':
        return None
    try:
        return int(value_str)
    except ValueError:
        return None


def extract_ev_charging_data_simple(connection, table_name):

    csv_file_path="Data/EV_with_stations.csv"

    try:
        cursor = connection.cursor()
        row_count = 0
        skipped_rows = 0
        
        # Use utf-8-sig to handle BOM if present
        with open(csv_file_path, 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            
            # Strip whitespace from column names
            csv_reader.fieldnames = [name.strip() for name in csv_reader.fieldnames]
            
            # Debug: Print column names to verify
#            print(f"CSV Columns found: {csv_reader.fieldnames}\n")
            
            for row_num, row in enumerate(csv_reader, start=2):
                try:
                    # Strip whitespace from all values
                    row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
                    
                    # Parse datetime fields
                    start_time = parse_csv_datetime(row['Charging Start Time'])
                    end_time = parse_csv_datetime(row['Charging End Time'])
                    
                    if start_time is None or end_time is None:
                        print(f"Skipping row {row_num} due to invalid datetime")
                        skipped_rows += 1
                        continue
                    
                    cursor.execute(
                        f"""INSERT INTO {table_name} 
                           (user_id, vehicle_model, battery_capacity_kwh, charging_station_id,
                            charging_start_time, charging_end_time, energy_consumed_kwh,
                            charging_duration_hours, charging_rate_kw, charging_cost_eur,
                            time_of_day, day_of_week, state_of_charge_start_pct,
                            state_of_charge_end_pct, distance_driven_km, temperature_c,
                            vehicle_age_years)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (row['User ID'], 
                         row['Vehicle Model'],
                         parse_decimal(row['Battery Capacity (kWh)']),
                         row['Charging Station ID'],
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
                         parse_decimal(row['Temperature (C)']),
                         parse_int(row['Vehicle Age (years)']))
                    )
                    row_count += 1
                    
                except KeyError as e:
                    print(f"Row {row_num}: Missing column {e}")
                    print(f"Available columns: {list(row.keys())}")
                    skipped_rows += 1
                    continue
                except Exception as e:
                    print(f"Row {row_num}: Error processing row: {e}")
                    skipped_rows += 1
                    continue
        
        connection.commit()
        print(f"\n=== Import Summary ===")
        print(f"Successfully imported: {row_count} rows")
        print(f"Skipped rows: {skipped_rows}")
        print(f"======================\n")
        cursor.close()
        return True
        
    except mysql.connector.Error as e:
        print(f"MySQL Error importing data: {e}")
        connection.rollback()
        return False
    except Exception as e:
        print(f"Error importing data: {e}")
        connection.rollback()
        return False

def extract_ev_stations(connection, table_name, batch_size=1000):

    csv_file_path="Data/EV-Stations_with_ids_coords.csv"

    try:
        cursor = connection.cursor()
        row_count = 0
        batch_data = []
        
        with open(csv_file_path, 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            csv_reader.fieldnames = [name.strip() for name in csv_reader.fieldnames]
            
            print(f"CSV Columns found: {csv_reader.fieldnames}\n")
            
            for row_num, row in enumerate(csv_reader, start=2):
                try:
                    row = {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
                    
                    # Add to batch instead of inserting immediately
                    batch_data.append((
                        row['Station ID'],
                        row['Distrito'],
                        row['Concelho'],
                        row['Freguesia'],
                        parse_decimal(row['Latitude']),
                        parse_decimal(row['Longitude']),
                        parse_decimal(row['Potência Máxima Admissível (kW)']),
                        parse_int(row['Pontos de ligação para instalações de PCVE'])
                    ))
                    
                    # Execute batch insert when batch_size is reached
                    if len(batch_data) >= batch_size:
                        cursor.executemany(
                            f"""INSERT IGNORE INTO {table_name} 
                               (station_id, distrito, concelho, freguesia, latitude, longitude,
                                potencia_maxima_kw, pontos_ligacao)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                            batch_data
                        )
                        row_count += len(batch_data)
                        print(f"Inserted batch: {row_count} stations processed")
                        batch_data = []
                        
                except Exception as e:
                    print(f"Row {row_num}: Error - {e}")
                    continue
            
            # Insert remaining batch
            if batch_data:
                cursor.executemany(
                    f"""INSERT IGNORE INTO {table_name} 
                       (station_id, distrito, concelho, freguesia, latitude, longitude,
                        potencia_maxima_kw, pontos_ligacao)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    batch_data
                )
                row_count += len(batch_data)
        
        connection.commit()
        print(f"\nSuccessfully imported: {row_count} stations")
        cursor.close()
        return True
        
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
        connection.rollback()
        return False


def import_both_datasets(connection):
    """
    Import both charging data and stations data.
    
    Args:
        charging_csv_path: Path to the charging data CSV
        stations_csv_path: Path to the stations CSV
        connection: MySQL connection object
    
    Returns:
        bool: True if both imports successful
    """
    print("=" * 60)
    print("IMPORTING CHARGING DATA")
    print("=" * 60)
    charging_success = extract_ev_charging_data_simple(
        connection, 
        "ev_charging_data"
    )
    
    print("\n" + "=" * 60)
    print("IMPORTING STATIONS DATA")
    print("=" * 60)
    stations_success = extract_ev_stations(
        connection, 
        "ev_stations"
    )
    
    return charging_success and stations_success
