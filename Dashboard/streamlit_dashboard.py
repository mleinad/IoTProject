import streamlit as st
import paho.mqtt.client as mqtt
import json
import pandas as pd
import plotly.express as px
from datetime import datetime
import threading
import time
import os
import sys

# Page config
st.set_page_config(
    page_title="EV Charging - Real-time Monitor",
    layout="wide",
    page_icon="🔌"
)

# MQTT Configuration  
BROKER_HOST = "mqtt-broker"
BROKER_PORT = 1883
TOPIC = "idc/fc01"

# File to store messages (persists across Streamlit reruns)
MESSAGE_FILE = "/tmp/mqtt_messages.jsonl"

# Initialize session state
if 'stats' not in st.session_state:
    st.session_state.stats = {
        "total_sessions": 0,
        "total_energy": 0.0,
        "total_cost": 0.0,
        "unique_users": set(),
        "unique_stations": set(),
        "last_update": "Never",
        "connected": False
    }
if 'mqtt_started' not in st.session_state:
    st.session_state.mqtt_started = False
if 'last_processed_line' not in st.session_state:
    st.session_state.last_processed_line = 0

# MQTT Callbacks - Write to file
def on_connect_callback(client, userdata, flags, reason_code, properties):
    """Callback when connected to MQTT broker."""
    print(f"🔌 on_connect: {reason_code}", flush=True)
    sys.stdout.flush()
    if reason_code == 0:
        # Write connection status to file
        with open(MESSAGE_FILE, 'a') as f:
            f.write(json.dumps({"type": "status", "connected": True}) + "\n")
        client.subscribe(TOPIC)
        print(f"✓ Subscribed to {TOPIC}", flush=True)
        sys.stdout.flush()

def on_message_callback(client, userdata, msg):
    """Callback when message received - Write to file."""
    try:
        payload = json.loads(msg.payload.decode())
        # Append to file
        with open(MESSAGE_FILE, 'a') as f:
            f.write(json.dumps({"type": "data", "payload": payload}) + "\n")
        print(f"📨 Saved: {payload['user_id']} - {payload['energy_consumed_kwh']} kWh", flush=True)
        sys.stdout.flush()
    except Exception as e:
        print(f"✗ Error: {e}", flush=True)
        sys.stdout.flush()

# Start MQTT client once
if not st.session_state.mqtt_started:
    # Create file if doesn't exist
    if not os.path.exists(MESSAGE_FILE):
        open(MESSAGE_FILE, 'w').close()
    
    print("="*50, flush=True)
    print("🚀 STARTING MQTT CLIENT", flush=True)
    print(f"📡 Broker: {BROKER_HOST}:{BROKER_PORT}", flush=True)
    print(f"📢 Topic: {TOPIC}", flush=True)
    print(f"📁 Storage: {MESSAGE_FILE}", flush=True)
    print("="*50, flush=True)
    sys.stdout.flush()
    
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dashboard_file_storage")
    client.on_connect = on_connect_callback
    client.on_message = on_message_callback
    
    def mqtt_loop():
        try:
            print("🔌 Connecting...", flush=True)
            sys.stdout.flush()
            client.connect(BROKER_HOST, BROKER_PORT, 60)
            print("🔄 Loop started", flush=True)
            sys.stdout.flush()
            client.loop_forever()
        except Exception as e:
            print(f"✗ Error: {e}", flush=True)
            sys.stdout.flush()
    
    thread = threading.Thread(target=mqtt_loop, daemon=True)
    thread.start()
    st.session_state.mqtt_started = True
    time.sleep(1)

# Process new messages from file
processed = 0
all_messages = []

if os.path.exists(MESSAGE_FILE):
    with open(MESSAGE_FILE, 'r') as f:
        lines = f.readlines()
        
    # Process only new lines since last refresh
    new_lines = lines[st.session_state.last_processed_line:]
    
    for line in new_lines:
        try:
            item = json.loads(line.strip())
            
            if item["type"] == "status":
                st.session_state.stats["connected"] = item["connected"]
            
            elif item["type"] == "data":
                payload = item["payload"]
                all_messages.append(payload)
                
                st.session_state.stats["total_sessions"] += 1
                st.session_state.stats["total_energy"] += payload.get("energy_consumed_kwh", 0)
                st.session_state.stats["total_cost"] += payload.get("charging_cost_eur", 0)
                st.session_state.stats["unique_users"].add(payload["user_id"])
                st.session_state.stats["unique_stations"].add(payload["station_id"])
                st.session_state.stats["last_update"] = datetime.now().strftime('%H:%M:%S')
                
                processed += 1
        except:
            pass
    
    # Update last processed line
    st.session_state.last_processed_line = len(lines)

if processed > 0:
    print(f"✅ Processed {processed} new messages", flush=True)
    sys.stdout.flush()

# Store all messages for display (keep last 100)
if 'all_messages_display' not in st.session_state:
    st.session_state.all_messages_display = []

st.session_state.all_messages_display.extend(all_messages)
if len(st.session_state.all_messages_display) > 100:
    st.session_state.all_messages_display = st.session_state.all_messages_display[-100:]

# UI
st.title("🔌 EV Charging Network - Real-time Monitoring")

connection_status = "🟢 Connected" if st.session_state.stats["connected"] else "🔴 Disconnected"
st.markdown(f"""
**MQTT Topic:** `{TOPIC}` | **Broker:** `{BROKER_HOST}:{BROKER_PORT}` | 
**Status:** {connection_status} | **Last Updated:** {st.session_state.stats["last_update"]}
""")

col1, col2 = st.columns([1, 9])
with col1:
    if st.button("🔄 Refresh"):
        st.rerun()

if processed > 0:
    st.success(f"✅ Just processed {processed} new messages!")

st.markdown("---")

# Metrics
st.subheader("📊 Real-time Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

stats = st.session_state.stats

with col1:
    st.metric("Total Sessions", stats["total_sessions"])
with col2:
    st.metric("Total Energy (kWh)", f"{stats['total_energy']:.2f}")
with col3:
    st.metric("Total Cost (€)", f"€{stats['total_cost']:.2f}")
with col4:
    st.metric("Active Users", len(stats["unique_users"]))
with col5:
    st.metric("Active Stations", len(stats["unique_stations"]))

st.markdown("---")

# Sessions
st.subheader("📋 Recent Charging Sessions")

if len(st.session_state.all_messages_display) > 0:
    df = pd.DataFrame(st.session_state.all_messages_display)
    
    display_cols = ['timestamp', 'user_id', 'station_id', 'energy_consumed_kwh', 
                   'charging_cost_eur', 'charging_duration_hours', 'time_of_day']
    
    if all(col in df.columns for col in display_cols):
        df_display = df[display_cols].tail(20).iloc[::-1]
        
        st.dataframe(
            df_display,
            width='stretch',
            hide_index=True,
            column_config={
                "timestamp": "Time",
                "user_id": "User",
                "station_id": "Station",
                "energy_consumed_kwh": st.column_config.NumberColumn("Energy (kWh)", format="%.2f"),
                "charging_cost_eur": st.column_config.NumberColumn("Cost (€)", format="€%.2f"),
                "charging_duration_hours": st.column_config.NumberColumn("Duration (h)", format="%.2f"),
                "time_of_day": "Time of Day"
            }
        )
        
        # Charts
        st.markdown("### 📈 Analytics")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.line(df.reset_index().tail(50), x='index', y='energy_consumed_kwh',
                         title="⚡ Energy Consumption")
            st.plotly_chart(fig, width='stretch')
        with col2:
            fig = px.histogram(df.tail(50), x='charging_cost_eur', nbins=15,
                             title="💰 Cost Distribution")
            st.plotly_chart(fig, width='stretch')
else:
    st.info("⏳ Waiting for data...")
    st.code("# Run: cd Communication && python3 mqtt_publisher.py\n# Then click Refresh")

# Sidebar
st.sidebar.title("🔌 System Info")
st.sidebar.info(f"""
**Stage 1 - Real-time Monitoring**

**Statistics:**
- Sessions: {stats['total_sessions']}
- Energy: {stats['total_energy']:.2f} kWh
- Cost: €{stats['total_cost']:.2f}
- Users: {len(stats['unique_users'])}
- Stations: {len(stats['unique_stations'])}

**Messages:** {len(st.session_state.all_messages_display)}  
**Status:** {connection_status}
""")

auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=False)
if auto_refresh:
    time.sleep(5)
    st.rerun()
