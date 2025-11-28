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
        self.client = mqtt.Client(client_id="ev_publisher", protocol=mqtt.MQTTv311)
        
        # Load offline dataset for realistic simulation
        csv_path = "Data/EV_with_stations.csv"
        if not os.path.exists(csv_path):
            csv_path = "../Data/EV_with_stations.csv"
        
        print(f"📁 Loading dataset from: {csv_path}")
        self.dataset = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig')
        print(f"✓ Loaded {len(self.dataset)} records")
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"✓ Connected to MQTT Broker at {self.broker_host}:{self.broker_port}")
        else:
            print(f"✗ Failed to connect. Return code: {rc}")
    
    def connect(self):
        self.client.on_connect = self.on_connect
        print(f"🔌 Connecting to MQTT Broker...")
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.client.loop_start()
        time.sleep(2)  # Wait for connection
    
    def publish_charging_session(self, session_data):
        """Publish a single charging session to MQTT."""
        message = json.dumps(session_data)
        result = self.client.publish(self.topic, message, qos=1)
        
        if result.rc == 0:
            print(f"✓ Published: User {session_data['user_id']} | Station {session_data['station_id']} | {session_data['energy_consumed_kwh']} kWh")
        else:
            print(f"✗ Failed to publish message")
        
        return result.rc == 0
    
    def simulate_realtime_data(self, interval_seconds=3, num_sessions=100):
        """Simulate real-time charging sessions from offline dataset."""
        print(f"\n🚗 Starting real-time simulation...")
        print(f"📡 Publishing to topic: {self.topic}")
        print(f"⏱️  Interval: {interval_seconds} seconds")
        print(f"📊 Number of sessions: {num_sessions}\n")
        
        for i in range(num_sessions):
            # Get random row from dataset
            row = self.dataset.sample(n=1).iloc[0]
            
            # Helper function to parse values
            def parse_value(val, default=0):
                if pd.isna(val) or val == '':
                    return default
                return float(str(val).replace(',', '.'))
            
            # Create message payload
            session_data = {
                "timestamp": datetime.now().isoformat(),
                "session_id": f"SESSION_{int(time.time())}_{i}",
                "user_id": str(row['User ID']),
                "vehicle_model": str(row['Vehicle Model']),
                "battery_capacity_kwh": parse_value(row['Battery Capacity (kWh)']),
                "station_id": str(row['Charging Station ID']),
                "charging_start_time": datetime.now().strftime('%d/%m/%y %H:%M'),
                "energy_consumed_kwh": parse_value(row['Energy Consumed (kWh)']),
                "charging_duration_hours": parse_value(row['Charging Duration (hours)']),
                "charging_rate_kw": parse_value(row['Charging Rate (kW)']),
                "charging_cost_eur": parse_value(row['Charging Cost (EUR)']),
                "time_of_day": str(row['Time of Day']),
                "day_of_week": str(row['Day of Week']),
                "state_of_charge_start_pct": parse_value(row['State of Charge (Start %)']),
                "temperature_c": parse_value(row['Temperature (C)'])
            }
            
            self.publish_charging_session(session_data)
            time.sleep(interval_seconds)
    
    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
        print("\n✓ Disconnected from MQTT Broker")


if __name__ == "__main__":
    # Configuration - use env vars when in Docker
    BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
    BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
    TOPIC = os.getenv("MQTT_TOPIC", "idc/fc01")
    
    publisher = EVChargingPublisher(BROKER_HOST, BROKER_PORT, TOPIC)
    publisher.connect()
    
    try:
        publisher.simulate_realtime_data(interval_seconds=2, num_sessions=50)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    finally:
        publisher.disconnect()
