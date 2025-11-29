import paho.mqtt.client as mqtt
import json
import os
import sys
import mysql.connector
from datetime import datetime
from collections import deque

# Add parent directory to path
sys.path.append('/app')

# In-memory storage
charging_sessions_buffer = deque(maxlen=1000)
session_stats = {
    "total_sessions": 0,
    "total_energy_kwh": 0,
    "total_cost_eur": 0,
    "active_users": set(),
    "active_stations": set(),
    "sessions_by_time_of_day": {"Morning": 0, "Afternoon": 0, "Evening": 0, "Night": 0},
    "sessions_by_day": {},
    "recent_sessions": []
}

class MQTTProcessor:
    def __init__(self, broker_host, broker_port, topic):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        
        # MQTT setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="processor", protocol=mqtt.MQTTv311)
        
        # MySQL setup
        self.db_connection = None
        self.db_cursor = None
        self.setup_database()
        
    def setup_database(self):
        """Setup MySQL database connection and clear old data."""
        try:
            self.db_connection = mysql.connector.connect(
                host=os.getenv("MYSQL_DB_HOST", "mysql-db"),
                user=os.getenv("MYSQL_DB_USER", "iot_user"),
                password=os.getenv("MYSQL_DB_PASSWORD", "iot_password"),
                database=os.getenv("MYSQL_DATABASE", "IoT_project"),
                autocommit=True
            )
            self.db_cursor = self.db_connection.cursor()
            print("✓ Connected to MySQL Database")
            
            # NEW: Clear old data for fresh demo
            self.clear_old_data()
            
            # Ensure tables exist
            self.ensure_tables_exist()
            
        except Exception as e:
            print(f"⚠️  Database connection failed: {e}")
            print("   Processor will continue without database persistence")
            self.db_connection = None

    def clear_old_data(self):
        """Clear old charging data for fresh demo."""
        try:
            print("🧹 Clearing old charging data...")
            self.db_cursor.execute("TRUNCATE TABLE ev_charging_data")
            print("✓ Table cleared - starting fresh!")
        except Exception as e:
            print(f"⚠️  Could not clear old data: {e}")

    def ensure_tables_exist(self):
        """Create charging_sessions table if it doesn't exist."""
        try:
            self.db_cursor.execute("""
                CREATE TABLE IF NOT EXISTS ev_charging_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    vehicle_model VARCHAR(100),
                    battery_capacity_kwh DECIMAL(5,2),
                    charging_station_id VARCHAR(50),
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
                    INDEX idx_station_id (charging_station_id),
                    INDEX idx_vehicle_model (vehicle_model),
                    INDEX idx_day_week (day_of_week),
                    INDEX idx_time_of_day (time_of_day)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            print("✓ ev_charging_data table verified/created")
        
        except Exception as e:
            print(f"⚠️  Could not verify table: {e}")

    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback when connected to MQTT broker."""
        if reason_code == 0:
            print(f"✓ Processor connected to MQTT Broker at {self.broker_host}:{self.broker_port}")
            client.subscribe(self.topic)
            print(f"✓ Subscribed to topic: {self.topic}")
            print(f"🎧 Waiting for messages...")
        else:
            print(f"✗ Connection failed with code {reason_code}")
    
    def on_message(self, client, userdata, msg):
        """Callback when message received."""
        try:
            payload = json.loads(msg.payload.decode())
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"📨 [{timestamp}] Received: User {payload['user_id']} - {payload['energy_consumed_kwh']:.2f} kWh at {payload['station_id']}")
            
            # Process in memory
            self.process_charging_session(payload)
            
            # Save to database
            self.save_to_database(payload)
            
        except Exception as e:
            print(f"✗ Error processing message: {e}")
    
    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Callback when disconnected."""
        print(f"⚠️  Disconnected from broker. Reason code: {reason_code}")
        if reason_code != 0:
            print("🔄 Attempting to reconnect...")
    
    def process_charging_session(self, session_data):
        """Process incoming charging session and update in-memory stats."""
        global session_stats
        
        # Add to buffer
        charging_sessions_buffer.append(session_data)
        
        # Update statistics
        session_stats["total_sessions"] += 1
        session_stats["total_energy_kwh"] += session_data.get("energy_consumed_kwh", 0)
        session_stats["total_cost_eur"] += session_data.get("charging_cost_eur", 0)
        session_stats["active_users"].add(session_data["user_id"])
        session_stats["active_stations"].add(session_data["station_id"])
        
        # Update time of day distribution
        time_of_day = session_data.get("time_of_day", "Unknown")
        if time_of_day in session_stats["sessions_by_time_of_day"]:
            session_stats["sessions_by_time_of_day"][time_of_day] += 1
        
        # Update day of week
        day = session_data.get("day_of_week", "Unknown")
        session_stats["sessions_by_day"][day] = session_stats["sessions_by_day"].get(day, 0) + 1
        
        # Keep last 100 sessions
        session_stats["recent_sessions"].append(session_data)
        if len(session_stats["recent_sessions"]) > 100:
            session_stats["recent_sessions"].pop(0)
        
        # Print current stats
        print(f"📊 Total Sessions: {session_stats['total_sessions']} | "
              f"Energy: {session_stats['total_energy_kwh']:.2f} kWh | "
              f"Cost: €{session_stats['total_cost_eur']:.2f}")
    
    def save_to_database(self, session_data):
        """Save charging session to MySQL database."""
        if not self.db_connection or not self.db_cursor:
            return
        
        try:
            # Insert into ev_charging_data table (not charging_sessions)
            query = """
                INSERT INTO ev_charging_data 
                (vehicle_model, battery_capacity_kwh, charging_station_id,
                energy_consumed_kwh, charging_duration_hours, charging_rate_kw,
                charging_cost_eur, time_of_day, day_of_week,
                state_of_charge_start_pct, state_of_charge_end_pct,
                distance_driven_km, temperature_c, vehicle_age_years)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                session_data.get('vehicle_model', 'Unknown'),
                session_data.get('battery_capacity_kwh', 0),
                session_data.get('station_id', 'Unknown'),
                session_data.get('energy_consumed_kwh', 0),
                session_data.get('charging_duration_hours', 0),
                session_data.get('charging_rate_kw', 0),
                session_data.get('charging_cost_eur', 0),
                session_data.get('time_of_day', 'Unknown'),
                session_data.get('day_of_week', 'Unknown'),
                session_data.get('state_of_charge_start_pct', 0),
                session_data.get('state_of_charge_end_pct', 0),
                session_data.get('distance_driven_km', 0),
                session_data.get('temperature_c', 0),
                session_data.get('vehicle_age_years', 0)
            )
            
            self.db_cursor.execute(query, values)
            print(f"💾 Saved to DB: {session_data.get('user_id', 'Unknown')} - "
                f"{session_data.get('energy_consumed_kwh', 0):.2f} kWh")
            
        except Exception as e:
            print(f"✗ Database save error: {e}")
            # Try to reconnect if connection was lost
            if "MySQL Connection not available" in str(e):
                self.setup_database()

    
    def start(self):
        """Start the MQTT processor."""
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        print(f"🚀 Starting MQTT Processor...")
        print(f"📡 Connecting to {self.broker_host}:{self.broker_port}")
        
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_forever()
        except Exception as e:
            print(f"✗ Failed to connect: {e}")
        finally:
            if self.db_connection:
                self.db_connection.close()
                print("Database connection closed")


if __name__ == "__main__":
    BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mqtt-broker")
    BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
    TOPIC = os.getenv("MQTT_TOPIC", "idc/fc01")
    
    processor = MQTTProcessor(BROKER_HOST, BROKER_PORT, TOPIC)
    processor.start()
