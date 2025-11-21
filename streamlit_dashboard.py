import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv
from Database.mysql_connector import connect_mysql
from Database.queries_fixed import *

# Page configuration
st.set_page_config(
    page_title="EV Charging Analytics",
    page_icon="🔌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

# Database connection helpers
def get_fresh_connection():
    """Get a fresh database connection."""
    load_dotenv()
    return connect_mysql([
        os.getenv("MYSQL_DB_HOST", "localhost"),
        os.getenv("MYSQL_DB_USER"),
        os.getenv("MYSQL_DB_PASSWORD")
    ], "IoT_project")

def safe_execute_query(query, params=None):
    """Execute query safely with connection handling."""
    conn = None
    try:
        conn = get_fresh_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        st.error(f"Query error: {e}")
        return []
    finally:
        if conn:
            conn.close()

def safe_query_function(query_func, *args):
    """Execute a query function safely."""
    conn = None
    try:
        conn = get_fresh_connection()
        result = query_func(conn, *args)
        return result if result else []
    except Exception as e:
        st.error(f"Query error: {e}")
        return []
    finally:
        if conn:
            conn.close()

# Sidebar
#st.sidebar.image("https://ibb.co/1fZDygYp", width=100)
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Page",
    ["📊 Overview", "📈 Trends Analysis", "👥 User Analytics", 
     "🏢 Station Analytics", "🗺️ Geographic Analysis", "📋 Raw Data"]
)

st.sidebar.markdown("---")
st.sidebar.info("""
    **EV Charging Dashboard**  
    Analyze charging patterns, energy consumption, 
    and station utilization across Portugal.
""")

# ============================================
# PAGE 1: OVERVIEW
# ============================================
if page == "📊 Overview":
    st.markdown('<p class="main-header">🔌 EV Charging Analytics Dashboard</p>', unsafe_allow_html=True)
    
    # Key Metrics
    st.subheader("📌 Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        result = safe_execute_query("SELECT COUNT(*) FROM ev_charging_data")
        if result and len(result) > 0:
            st.metric("Total Sessions", f"{result[0][0]:,}")
    
    with col2:
        result = safe_execute_query("SELECT COUNT(DISTINCT user_id) FROM ev_charging_data")
        if result and len(result) > 0:
            st.metric("Active Users", f"{result[0][0]}")
    
    with col3:
        result = safe_execute_query("SELECT COUNT(*) FROM ev_stations")
        if result and len(result) > 0:
            st.metric("Total Stations", f"{result[0][0]:,}")
    
    with col4:
        result = safe_execute_query("SELECT ROUND(SUM(energy_consumed_kwh), 0) FROM ev_charging_data")
        if result and len(result) > 0 and result[0][0]:
            st.metric("Total Energy (kWh)", f"{result[0][0]:,}")
    
    st.markdown("---")
    
    # Energy & Cost Overview
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💡 Energy Statistics")
        energy_stats = safe_query_function(get_total_energy_delivered)
        if energy_stats and len(energy_stats) > 0:
            energy_stats = energy_stats[0]
            data = {
                "Metric": ["Total Sessions", "Total Energy", "Average Energy", "Min Energy", "Max Energy"],
                "Value": [
                    f"{energy_stats[0]:,}",
                    f"{energy_stats[1]:,} kWh" if energy_stats[1] else "N/A",
                    f"{energy_stats[2]} kWh" if energy_stats[2] else "N/A",
                    f"{energy_stats[3]} kWh" if energy_stats[3] else "N/A",
                    f"{energy_stats[4]} kWh" if energy_stats[4] else "N/A"
                ]
            }
            st.dataframe(pd.DataFrame(data), hide_index=True, width=600)
    
    with col2:
        st.subheader("💰 Cost Statistics")
        cost_stats = safe_query_function(get_cost_statistics)
        if cost_stats and len(cost_stats) > 0:
            cost_stats = cost_stats[0]
            data = {
                "Metric": ["Total Sessions", "Average Cost", "Min Cost", "Max Cost", "Total Revenue"],
                "Value": [
                    f"{cost_stats[0]:,}",
                    f"€{cost_stats[1]}" if cost_stats[1] else "N/A",
                    f"€{cost_stats[2]}" if cost_stats[2] else "N/A",
                    f"€{cost_stats[3]}" if cost_stats[3] else "N/A",
                    f"€{cost_stats[4]:,}" if cost_stats[4] else "N/A"
                ]
            }
            st.dataframe(pd.DataFrame(data), hide_index=True, width=600)
    
    st.markdown("---")
    
    # Time of Day Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⏰ Charging by Time of Day")
        data = safe_query_function(get_time_of_day_distribution)
        if data and len(data) > 0:
            df = pd.DataFrame(data, columns=['Time of Day', 'Sessions', 'Percentage', 'Avg Cost', 'Avg Energy', 'Avg Duration'])
            fig = px.pie(df, values='Sessions', names='Time of Day', 
                         title='Session Distribution by Time of Day',
                         color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig, width='stretch')
    
    with col2:
        st.subheader("📅 Charging by Day of Week")
        data = safe_query_function(get_day_of_week_distribution)
        if data and len(data) > 0:
            df = pd.DataFrame(data, columns=['Day', 'Sessions', 'Percentage', 'Avg Cost', 'Avg Energy'])
            fig = px.bar(df, x='Day', y='Sessions', 
                         title='Sessions by Day of Week',
                         color='Sessions',
                         color_continuous_scale='Blues')
            st.plotly_chart(fig, width='stretch')

# ============================================
# PAGE 2: TRENDS ANALYSIS
# ============================================
elif page == "📈 Trends Analysis":
    st.markdown('<p class="main-header">📈 Trends Analysis</p>', unsafe_allow_html=True)
    
    trend_type = st.radio("Select Trend Period", ["Daily", "Weekly", "Monthly"], horizontal=True)
    
    if trend_type == "Daily":
        data = safe_query_function(get_daily_trends)
        if data and len(data) > 0:
            df = pd.DataFrame(data, columns=['Date', 'Sessions', 'Total Energy', 'Avg Energy', 
                                             'Revenue', 'Avg Cost', 'Avg Duration', 'Users', 'Stations'])
            
            # Line chart
            fig = px.line(df, x='Date', y='Sessions', title='Daily Sessions Trend')
            st.plotly_chart(fig, width='stretch')
            
            # Data table
            st.subheader("📋 Detailed Data")
            st.dataframe(df, hide_index=True, width='stretch')
    
    elif trend_type == "Weekly":
        data = safe_query_function(get_weekly_trends)
        if data and len(data) > 0:
            df = pd.DataFrame(data, columns=['Year-Week', 'Week Start', 'Week End', 'Sessions',
                                            'Total Energy', 'Avg Energy', 'Revenue', 'Avg Cost', 
                                            'Avg Duration', 'Users'])
            
            # Line chart
            fig = px.line(df, x='Year-Week', y='Sessions', title='Weekly Sessions Trend')
            st.plotly_chart(fig, width='stretch')
            
            # Data table
            st.subheader("📋 Detailed Data")
            st.dataframe(df, hide_index=True, width='stretch')

    else:  # Monthly
        data = safe_query_function(get_monthly_trends)
        if data and len(data) > 0:
            df = pd.DataFrame(data, columns=['Month', 'Month Start', 'Month End', 'Sessions',
                                            'Total Energy', 'Avg Energy', 'Revenue', 'Avg Cost',
                                            'Avg Duration', 'Users', 'Stations'])
            
            # Line chart
            fig = px.line(df, x='Month', y='Sessions', title='Monthly Sessions Trend')
            st.plotly_chart(fig, width='stretch')
            
            # Data table
            st.subheader("📋 Detailed Data")
            st.dataframe(df, hide_index=True, width='stretch')

# ============================================
# PAGE 3: USER ANALYTICS
# ============================================
elif page == "👥 User Analytics":
    st.markdown('<p class="main-header">👥 User Analytics</p>', unsafe_allow_html=True)
    
    st.subheader("📊 Complete User Summary")
    data = safe_query_function(get_usage_per_user)
    if data and len(data) > 0:
        df = pd.DataFrame(data, columns=['User', 'Vehicle', 'Sessions', 'Total Spent', 'Avg Cost',
                                         'Total Energy', 'Avg Energy', 'Avg Duration', 'Stations Used', 'Days Active'])
        
        # Top users chart
        fig = px.bar(df.head(10), x='User', y='Sessions', color='Vehicle',
                     title='Top 10 Users by Sessions')
        st.plotly_chart(fig, width='stretch')
        
        # Full table
        st.dataframe(df, hide_index=True, width='stretch')

# ============================================
# PAGE 4: STATION ANALYTICS
# ============================================
elif page == "🏢 Station Analytics":
    st.markdown('<p class="main-header">🏢 Station Analytics</p>', unsafe_allow_html=True)
    
    st.subheader("⚡ Station Power Distribution")
    data = safe_query_function(get_power_distribution)
    if data and len(data) > 0:
        df = pd.DataFrame(data, columns=['Power Category', 'Station Count', 'Percentage', 'Avg Power', 'Connection Points'])
        
        fig = px.pie(df, values='Station Count', names='Power Category',
                     title='Stations by Power Category')
        st.plotly_chart(fig, width='stretch')
        
        st.dataframe(df, hide_index=True, width='stretch')

# ============================================
# PAGE 5: GEOGRAPHIC ANALYSIS
# ============================================
elif page == "🗺️ Geographic Analysis":
    st.markdown('<p class="main-header">🗺️ Geographic Analysis</p>', unsafe_allow_html=True)
    
    st.subheader("🏙️ Stations by Distrito")
    data = safe_query_function(get_stations_by_distrito)
    if data and len(data) > 0:
        df = pd.DataFrame(data, columns=['Distrito', 'Stations', 'Avg Power', 'Min Power', 'Max Power', 'Connection Points'])
        
        fig = px.bar(df.head(10), x='Distrito', y='Stations',
                     title='Top 10 Distritos by Station Count',
                     color='Avg Power', color_continuous_scale='Plasma')
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, width='stretch')
        
        st.dataframe(df, hide_index=True, width='stretch')

# ============================================
# PAGE 6: RAW DATA
# ============================================
elif page == "📋 Raw Data":
    st.markdown('<p class="main-header">📋 Raw Data Explorer</p>', unsafe_allow_html=True)
    
    limit = st.slider("Number of rows", 10, 500, 100)
    
    conn = get_fresh_connection()
    df = pd.read_sql(f"SELECT * FROM ev_charging_data ORDER BY charging_start_time DESC LIMIT {limit}", conn)
    conn.close()
    
    st.dataframe(df, hide_index=True, width='stretch')


