import streamlit as st
import paho.mqtt.client as mqtt
import json
import pandas as pd
import plotly.express as px
from datetime import datetime
import threading
import time

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

# Global shared state (accessible from MQTT thread)
if 'shared_data' not in st.session_state:
    st.session_state.shared_data = {
        "messages": [],
        "total_sessions": 0,
        "total_energy": 0.0,
        "total_cost": 0.0,
        "unique_users": set(),
        "unique_stations": set(),
        "connected": False,
        "last_update": "Never"
    }

if 'mqtt_started' not in st.session_state:
    st.session_state.mqtt_started = False

# MQTT Callbacks
def on_connect(client, userdata, flags, reason_code, properties):
    """Callback when connected to MQTT broker."""
    if reason_code == 0:
        st.session_state.shared_data["connected"] = True
        client.subscribe(TOPIC)
        print(f"✓ Dashboard connected and subscribed to {TOPIC}")
    else:
        st.session_state.shared_data["connected"] = False
        print(f"✗ Connection failed: {reason_code}")

def on_message(client, userdata, msg):
    """Callback when message received."""
    try:
        payload = json.loads(msg.payload.decode())
        
        # Update shared data
        st.session_state.shared_data["messages"].append(payload)
        if len(st.session_state.shared_data["messages"]) > 100:
            st.session_state.shared_data["messages"].pop(0)
        
        st.session_state.shared_data["total_sessions"] += 1
        st.session_state.shared_data["total_energy"] += payload.get("energy_consumed_kwh", 0)
        st.session_state.shared_data["total_cost"] += payload.get("charging_cost_eur", 0)
        st.session_state.shared_data["unique_users"].add(payload["user_id"])
        st.session_state.shared_data["unique_stations"].add(payload["station_id"])
        st.session_state.shared_data["last_update"] = datetime.now().strftime('%H:%M:%S')
        
        print(f"📨 Received: {payload['user_id']} - {payload['energy_consumed_kwh']} kWh")
        
    except Exception as e:
        print(f"✗ Error: {e}")

# Start MQTT client only once
if not st.session_state.mqtt_started:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dashboard_v2")
    client.on_connect = on_connect
    client.on_message = on_message
    
    def mqtt_loop():
        try:
            print(f"🔌 Connecting to {BROKER_HOST}:{BROKER_PORT}")
            client.connect(BROKER_HOST, BROKER_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print(f"✗ MQTT error: {e}")
    
    thread = threading.Thread(target=mqtt_loop, daemon=True)
    thread.start()
    st.session_state.mqtt_started = True
    time.sleep(2)

# UI
st.title("🔌 EV Charging Network - Real-time Monitoring")

# Status
connection_status = "🟢 Connected" if st.session_state.shared_data["connected"] else "🔴 Disconnected"
st.markdown(f"""
**MQTT Topic:** `{TOPIC}` | **Broker:** `{BROKER_HOST}:{BROKER_PORT}` | 
**Status:** {connection_status} | **Last Updated:** {st.session_state.shared_data["last_update"]}
""")

col1, col2 = st.columns([1, 9])
with col1:
    if st.button("🔄 Refresh"):
        st.rerun()

st.markdown("---")

# Metrics
st.subheader("📊 Real-time Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

data = st.session_state.shared_data

with col1:
    st.metric("Total Sessions", data["total_sessions"])

with col2:
    st.metric("Total Energy (kWh)", f"{data['total_energy']:.2f}")

with col3:
    st.metric("Total Cost (€)", f"€{data['total_cost']:.2f}")

with col4:
    st.metric("Active Users", len(data["unique_users"]))

with col5:
    st.metric("Active Stations", len(data["unique_stations"]))

st.markdown("---")

# Sessions Table
st.subheader("📋 Recent Charging Sessions")

if len(data["messages"]) > 0:
    df = pd.DataFrame(data["messages"])
    
    display_cols = ['timestamp', 'user_id', 'station_id', 'energy_consumed_kwh', 
                   'charging_cost_eur', 'charging_duration_hours', 'time_of_day']
    
    if all(col in df.columns for col in display_cols):
        df_display = df[display_cols].tail(20)
        
        st.dataframe(
            df_display,
            use_container_width=True,
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
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**⚡ Energy Trend**")
            fig = px.line(df.reset_index().tail(50), x='index', y='energy_consumed_kwh',
                         labels={'index': 'Session #', 'energy_consumed_kwh': 'Energy (kWh)'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("**💰 Cost Distribution**")
            fig = px.histogram(df.tail(50), x='charging_cost_eur', nbins=15,
                             labels={'charging_cost_eur': 'Cost (€)'})
            st.plotly_chart(fig, use_container_width=True)
else:
    st.info("⏳ Waiting for data from MQTT...")
    st.markdown(f"""
    **Connection Status:** {connection_status}
    
    **To start receiving data:**
    1. Run: `cd Communication && python3 mqtt_publisher.py`
    2. Click the Refresh button above to update display
    """)

# Sidebar
st.sidebar.title("🔌 System Info")
st.sidebar.info(f"""
**Project:** EV Charging IoT Platform  
**Stage:** 1 - Real-time Monitoring

**Stats:**
- Sessions: {data['total_sessions']}
- Energy: {data['total_energy']:.2f} kWh
- Cost: €{data['total_cost']:.2f}
- Users: {len(data['unique_users'])}
- Stations: {len(data['unique_stations'])}
""")

st.sidebar.markdown("---")
st.sidebar.caption("💡 Click Refresh to update the display with latest MQTT data")
