import paho.mqtt.client as mqtt
import json
import time
import pandas as pd
from datetime import datetime
import os


class EVChargingPublisher:
    def __init__(self, broker_host="localhost", broker_port=1883, topic="idc/fc01"):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        
        # Create MQTT client (compatible with old and new versions)
        try:
            # Try new API (paho-mqtt >= 2.0)
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 
                                     client_id="ev_publisher", 
                                     protocol=mqtt.MQTTv311)
            self.mqtt_version = 2
        except AttributeError:
            # Fall back to old API (paho-mqtt < 2.0)
            self.client = mqtt.Client(client_id="ev_publisher", 
                                     protocol=mqtt.MQTTv311)
            self.mqtt_version = 1
        
        # Load offline dataset for realistic simulation
        csv_path = self.find_csv_file()
        
        print(f"📁 Loading dataset from: {csv_path}")
        self.dataset = self.load_dataset(csv_path)
        print(f"✓ Loaded {len(self.dataset)} records")
        print(f"✓ Columns: {list(self.dataset.columns)}\n")
    
    def find_csv_file(self):
        """Find the CSV file in possible locations."""
        possible_paths = [
            "Data/EV_with_stations_for_online_simulation.csv",
            "../Data/EV_with_stations_for_online_simulation.csv",
            "Data/EV_with_stations.csv",
            "../Data/EV_with_stations.csv"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        raise FileNotFoundError(f"CSV file not found in any of these locations: {possible_paths}")
    
    def load_dataset(self, csv_path):
        """Load and clean the dataset."""
        # Read with semicolon delimiter
        df = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig')
        
        # Clean column names (remove extra spaces)
        df.columns = df.columns.str.strip()
        
        # Handle NaN values
        df = df.fillna({
            'User ID': 'Unknown',
            'Vehicle Model': 'Unknown',
            'Battery Capacity (kWh)': 0,
            'Charging Station ID': 'Unknown',
            'Energy Consumed (kWh)': 0,
            'Charging Duration (hours)': 0,
            'Charging Rate (kW)': 0,
            'Charging Cost (EUR)': 0,
            'Time of Day': 'Unknown',
            'Day of Week': 'Unknown',
            'State of Charge (Start %)': 0,
            'State of Charge (End %)': 0,
            'Distance Driven (since last charge) (km)': 0,
            'Temperature (C)': 0,
            'Vehicle Age (years)': 0
        })
        
        return df
        
    def on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback when connected to MQTT broker (compatible with both versions)."""
        if rc == 0:
            print(f"✓ Connected to MQTT Broker at {self.broker_host}:{self.broker_port}")
        else:
            print(f"✗ Failed to connect. Return code: {rc}")
    
    def connect(self):
        """Connect to MQTT broker."""
        self.client.on_connect = self.on_connect
        print(f"🔌 Connecting to MQTT Broker at {self.broker_host}:{self.broker_port}...")
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.client.loop_start()
        time.sleep(2)  # Wait for connection
    
    def parse_value(self, val, default=0):
        """Parse numeric values from CSV, handling NaN and string formatting."""
        if pd.isna(val) or val == '' or val == 'nan':
            return default
        try:
            # Replace comma with dot for European number format
            return float(str(val).replace(',', '.'))
        except (ValueError, AttributeError):
            return default
    
    def create_session_message(self, row, session_index):
        """Create MQTT message from CSV row."""
        # Generate timestamp for real-time simulation
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create message payload matching processor expectations
        session_data = {
            # Identifiers
            "timestamp": timestamp,
            "session_id": f"SESSION_{int(time.time())}_{session_index}",
            "user_id": str(row.get('User ID', f'User_{session_index % 20 + 1}')),
            "station_id": str(row.get('Charging Station ID', f'PT-EVS{session_index:05d}')),
            
            # Vehicle info
            "vehicle_model": str(row.get('Vehicle Model', 'Unknown')),
            "battery_capacity_kwh": self.parse_value(row.get('Battery Capacity (kWh)')),
            "vehicle_age_years": int(self.parse_value(row.get('Vehicle Age (years)', 0))),
            
            # Charging data (required by processor)
            "energy_consumed_kwh": self.parse_value(row.get('Energy Consumed (kWh)')),
            "charging_duration_hours": self.parse_value(row.get('Charging Duration (hours)')),
            "charging_cost_eur": self.parse_value(row.get('Charging Cost (EUR)')),
            "charging_rate_kw": self.parse_value(row.get('Charging Rate (kW)')),
            
            # Context
            "time_of_day": str(row.get('Time of Day', 'Unknown')),
            "day_of_week": str(row.get('Day of Week', 'Unknown')),
            
            # Battery state
            "state_of_charge_start_pct": self.parse_value(row.get('State of Charge (Start %)')),
            "state_of_charge_end_pct": self.parse_value(row.get('State of Charge (End %)')),
            
            # Additional data
            "distance_driven_km": self.parse_value(row.get('Distance Driven (since last charge) (km)')),
            "temperature_c": self.parse_value(row.get('Temperature (C)'))
        }
        
        return session_data
    
    def publish_charging_session(self, session_data):
        """Publish a single charging data session to MQTT."""
        message = json.dumps(session_data)
        result = self.client.publish(self.topic, message, qos=1)
        
        if result.rc == 0:
            print(f"✓ Published: User {session_data['user_id']} | "
                  f"Station {session_data['station_id']} | "
                  f"{session_data['energy_consumed_kwh']:.2f} kWh | "
                  f"€{session_data['charging_cost_eur']:.2f}")
        else:
            print(f"✗ Failed to publish message")
        
        return result.rc == 0
    
    def simulate_realtime_data(self, interval_seconds=2, num_sessions=50):
        """Simulate real-time charging sessions from offline dataset."""
        print("=" * 70)
        print("🚗 STARTING REAL-TIME EV CHARGING SIMULATION")
        print("=" * 70)
        print(f"📡 MQTT Broker: {self.broker_host}:{self.broker_port}")
        print(f"📢 Topic: {self.topic}")
        print(f"⏱️  Interval: {interval_seconds} seconds between messages")
        print(f"📊 Total sessions to publish: {num_sessions}")
        print("=" * 70 + "\n")
        
        published_count = 0
        failed_count = 0
        
        for i in range(num_sessions):
            try:
                # Get random row from dataset
                row = self.dataset.sample(n=1).iloc[0]
                
                # Create message
                session_data = self.create_session_message(row, i)
                
                # Publish
                if self.publish_charging_session(session_data):
                    published_count += 1
                else:
                    failed_count += 1
                
                # Progress indicator
                if (i + 1) % 10 == 0:
                    print(f"\n📈 Progress: {i + 1}/{num_sessions} sessions published\n")
                
                # Wait before next message
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                print("\n⚠️  Interrupted by user")
                break
            except Exception as e:
                print(f"✗ Error publishing session {i}: {e}")
                failed_count += 1
        
        # Summary
        print("\n" + "=" * 70)
        print("📊 SIMULATION SUMMARY")
        print("=" * 70)
        print(f"✓ Successfully published: {published_count} sessions")
        if failed_count > 0:
            print(f"✗ Failed: {failed_count} sessions")
        print(f"⏱️  Total time: {published_count * interval_seconds:.1f} seconds")
        print("=" * 70 + "\n")
    
    def disconnect(self):
        """Disconnect from MQTT broker."""
        self.client.loop_stop()
        self.client.disconnect()
        print("✓ Disconnected from MQTT Broker\n")


if __name__ == "__main__":
    # Configuration - use env vars when in Docker, defaults for local
    BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
    BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
    TOPIC = os.getenv("MQTT_TOPIC", "idc/fc01")
    
    # Publisher settings
    INTERVAL_SECONDS = 0.5  # Time between messages
    NUM_SESSIONS = 90     # Total messages to send
    
    try:
        # Initialize publisher
        publisher = EVChargingPublisher(BROKER_HOST, BROKER_PORT, TOPIC)
        
        # Connect to broker
        publisher.connect()
        
        # Run simulation
        publisher.simulate_realtime_data(
            interval_seconds=INTERVAL_SECONDS,
            num_sessions=NUM_SESSIONS
        )
        
    except FileNotFoundError as e:
        print(f"\n✗ Error: {e}")
        print("Please ensure the CSV file exists in the Data/ folder")
    except ConnectionRefusedError:
        print(f"\n✗ Error: Could not connect to MQTT broker at {BROKER_HOST}:{BROKER_PORT}")
        print("Please ensure Docker services are running: docker-compose up -d")
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            publisher.disconnect()
        except:
            pass
