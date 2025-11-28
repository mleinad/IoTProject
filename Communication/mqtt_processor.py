import paho.mqtt.client as mqtt
import json
import os
import sys
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
        # Use VERSION2 API
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        
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
            print(f"📨 [{timestamp}] Received: User {payload['user_id']} - {payload['energy_consumed_kwh']} kWh at {payload['station_id']}")
            
            # Process the message
            self.process_charging_session(payload)
            
        except Exception as e:
            print(f"✗ Error processing message: {e}")
    
    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Callback when disconnected."""
        print(f"⚠️  Disconnected from broker. Reason code: {reason_code}")
        if reason_code != 0:
            print("🔄 Attempting to reconnect...")
    
    def process_charging_session(self, session_data):
        """Process incoming charging session and update stats."""
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


if __name__ == "__main__":
    BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "localhost")
    BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", 1883))
    TOPIC = os.getenv("MQTT_TOPIC", "idc/fc01")
    
    processor = MQTTProcessor(BROKER_HOST, BROKER_PORT, TOPIC)
    processor.start()
