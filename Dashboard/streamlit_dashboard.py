import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import mysql.connector
from datetime import datetime, timedelta
import os
import sqlalchemy
import time

# Page config
st.set_page_config(
    page_title="EV Charging Network Analytics",
    layout="wide",
    page_icon="⚡",
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
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .section-header {
        color: #1f77b4;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_database_connection():
    """Create and cache database connection."""
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "mysql-db"),
            user=os.getenv("MYSQL_USER", "iot_user"),
            password=os.getenv("MYSQL_PASSWORD", "iot_password"),
            database=os.getenv("MYSQL_DATABASE", "IoT_project")
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


@st.cache_data(ttl=30)
def load_charging_data():
    """Load charging data from database using SQLAlchemy."""
    host = os.getenv("MYSQL_HOST", "mysql-db")
    user = os.getenv("MYSQL_USER", "iot_user")
    password = os.getenv("MYSQL_PASSWORD", "iot_password")
    database = os.getenv("MYSQL_DATABASE", "IoT_project")
    
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    )
    
    query = """
        SELECT 
            id,
            vehicle_model,
            battery_capacity_kwh,
            charging_station_id as station_id,
            energy_consumed_kwh,
            charging_duration_hours,
            charging_rate_kw,
            charging_cost_eur,
            time_of_day,
            day_of_week,
            state_of_charge_start_pct,
            state_of_charge_end_pct,
            distance_driven_km,
            temperature_c,
            vehicle_age_years,
            created_at as timestamp
        FROM ev_charging_data
        ORDER BY created_at DESC
    """
    
    try:
        df = pd.read_sql(query, engine)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        df['hour'] = df['timestamp'].dt.hour
        df['week'] = df['timestamp'].dt.isocalendar().week
        df['month'] = df['timestamp'].dt.month
        df['year'] = df['timestamp'].dt.year
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def filter_data_by_period(df, period_type, custom_start=None, custom_end=None):
    """Filter data based on selected period."""
    if df.empty:
        return df
    
    now = datetime.now()
    
    if period_type == "Last 24 Hours":
        start_date = now - timedelta(days=1)
        return df[df['timestamp'] >= start_date]
    
    elif period_type == "Last 7 Days":
        start_date = now - timedelta(days=7)
        return df[df['timestamp'] >= start_date]
    
    elif period_type == "Last 30 Days":
        start_date = now - timedelta(days=30)
        return df[df['timestamp'] >= start_date]
    
    elif period_type == "Last 3 Months":
        start_date = now - timedelta(days=90)
        return df[df['timestamp'] >= start_date]
    
    elif period_type == "Last Year":
        start_date = now - timedelta(days=365)
        return df[df['timestamp'] >= start_date]
    
    elif period_type == "Custom Range" and custom_start and custom_end:
        return df[(df['timestamp'].dt.date >= custom_start) & 
                 (df['timestamp'].dt.date <= custom_end)]
    
    else:  # All Time
        return df


def create_time_series_chart(df, groupby_col, value_col, title):
    """Create time series chart."""
    if df.empty:
        return None
    
    grouped = df.groupby(groupby_col)[value_col].agg(['sum', 'mean', 'count']).reset_index()
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(
        go.Bar(x=grouped[groupby_col], y=grouped['sum'], name=f"Total {value_col}",
               marker_color='lightblue'),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(x=grouped[groupby_col], y=grouped['count'], name="Number of Sessions",
                  mode='lines+markers', marker_color='red', line=dict(width=2)),
        secondary_y=True
    )
    
    fig.update_layout(
        title=title,
        xaxis_title=groupby_col.replace('_', ' ').title(),
        hovermode='x unified',
        height=400
    )
    
    fig.update_yaxes(title_text=f"Total {value_col}", secondary_y=False)
    fig.update_yaxes(title_text="Number of Sessions", secondary_y=True)
    
    return fig


def main():
    # Header
    st.markdown('<p class="main-header">⚡ EV Charging Network Analytics Dashboard</p>', 
                unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading data from database..."):
        df = load_charging_data()
    
    if df.empty:
        st.error("No data available. Please ensure the database is populated.")
        st.info("💡 Run the MQTT publisher to generate data: `python Communication/mqtt_publisher.py`")
        return
    
    # Sidebar - Filters
    st.sidebar.header("🔍 Filters & Settings")
    
    # Period Selection
    st.sidebar.subheader("📅 Time Period")
    period_options = [
        "All Time",
        "Last 24 Hours",
        "Last 7 Days",
        "Last 30 Days",
        "Last 3 Months",
        "Last Year",
        "Custom Range"
    ]
    selected_period = st.sidebar.selectbox("Select Period", period_options)
    
    # Custom date range
    custom_start, custom_end = None, None
    if selected_period == "Custom Range":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            custom_start = st.date_input(
                "Start Date",
                value=df['timestamp'].min().date(),
                min_value=df['timestamp'].min().date(),
                max_value=df['timestamp'].max().date()
            )
        with col2:
            custom_end = st.date_input(
                "End Date",
                value=df['timestamp'].max().date(),
                min_value=df['timestamp'].min().date(),
                max_value=df['timestamp'].max().date()
            )
    
    # Filter data
    filtered_df = filter_data_by_period(df, selected_period, custom_start, custom_end)
    
    # Additional filters
    st.sidebar.subheader("🎯 Additional Filters")
    
    # Time of Day filter
    time_of_day_options = ['All'] + sorted(filtered_df['time_of_day'].unique().tolist())
    selected_time_of_day = st.sidebar.multiselect(
        "Time of Day",
        options=time_of_day_options,
        default=['All']
    )
    
    if 'All' not in selected_time_of_day and selected_time_of_day:
        filtered_df = filtered_df[filtered_df['time_of_day'].isin(selected_time_of_day)]
    
    # Vehicle Model filter
    vehicle_options = ['All'] + sorted(filtered_df['vehicle_model'].dropna().unique().tolist())
    selected_vehicles = st.sidebar.multiselect(
        "Vehicle Models",
        options=vehicle_options,
        default=['All']
    )
    
    if 'All' not in selected_vehicles and selected_vehicles:
        filtered_df = filtered_df[filtered_df['vehicle_model'].isin(selected_vehicles)]
    
    # Station filter
    station_options = ['All'] + sorted(filtered_df['station_id'].dropna().unique().tolist())[:50]  # Limit to 50
    selected_stations = st.sidebar.multiselect(
        "Charging Stations (Top 50)",
        options=station_options,
        default=['All']
    )
    
    if 'All' not in selected_stations and selected_stations:
        filtered_df = filtered_df[filtered_df['station_id'].isin(selected_stations)]
    
    # Refresh button
    if st.sidebar.toggle('Auto-refresh'):
        time.sleep(5)
        st.rerun()
    
    # Main Dashboard
    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Summary Statistics
    st.markdown('<h2 class="section-header">📊 Summary Statistics</h2>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_sessions = len(filtered_df)
        st.metric("Total Sessions", f"{total_sessions:,}")
    
    with col2:
        total_energy = filtered_df['energy_consumed_kwh'].sum()
        st.metric("Total Energy", f"{total_energy:,.2f} kWh")
    
    with col3:
        total_cost = filtered_df['charging_cost_eur'].sum()
        st.metric("Total Cost", f"€{total_cost:,.2f}")
    
    with col4:
        avg_duration = filtered_df['charging_duration_hours'].mean()
        st.metric("Avg Duration", f"{avg_duration:.2f} hrs")
    
    with col5:
        unique_stations = filtered_df['station_id'].nunique()
        st.metric("Active Stations", f"{unique_stations:,}")
    
    # a. Daily, Weekly, and Monthly Trends
    st.markdown('<h2 class="section-header">📈 Temporal Trends</h2>', unsafe_allow_html=True)
    
    trend_tab1, trend_tab2, trend_tab3 = st.tabs(["📅 Daily", "📆 Weekly", "📊 Monthly"])
    
    with trend_tab1:
        daily_chart = create_time_series_chart(
            filtered_df, 'date', 'energy_consumed_kwh',
            'Daily Charging Sessions and Energy Consumption'
        )
        if daily_chart:
            st.plotly_chart(daily_chart, width='stretch')
    
    with trend_tab2:
        weekly_data = filtered_df.groupby(['year', 'week']).agg({
            'energy_consumed_kwh': 'sum',
            'id': 'count'
        }).reset_index()
        weekly_data['week_label'] = weekly_data['year'].astype(str) + '-W' + weekly_data['week'].astype(str)
        
        fig_weekly = make_subplots(specs=[[{"secondary_y": True}]])
        fig_weekly.add_trace(
            go.Bar(x=weekly_data['week_label'], y=weekly_data['energy_consumed_kwh'],
                   name="Energy (kWh)", marker_color='lightgreen'),
            secondary_y=False
        )
        fig_weekly.add_trace(
            go.Scatter(x=weekly_data['week_label'], y=weekly_data['id'],
                      name="Sessions", mode='lines+markers', marker_color='orange'),
            secondary_y=True
        )
        fig_weekly.update_layout(title='Weekly Trends', height=400)
        fig_weekly.update_yaxes(title_text="Energy (kWh)", secondary_y=False)
        fig_weekly.update_yaxes(title_text="Sessions", secondary_y=True)
        st.plotly_chart(fig_weekly, width='stretch')
    
    with trend_tab3:
        monthly_data = filtered_df.groupby(['year', 'month']).agg({
            'energy_consumed_kwh': 'sum',
            'charging_cost_eur': 'sum',
            'id': 'count'
        }).reset_index()
        monthly_data['month_label'] = monthly_data['year'].astype(str) + '-' + monthly_data['month'].astype(str).str.zfill(2)
        
        fig_monthly = go.Figure()
        fig_monthly.add_trace(go.Bar(
            x=monthly_data['month_label'],
            y=monthly_data['energy_consumed_kwh'],
            name='Energy (kWh)',
            marker_color='steelblue'
        ))
        fig_monthly.update_layout(
            title='Monthly Energy Consumption',
            xaxis_title='Month',
            yaxis_title='Energy (kWh)',
            height=400
        )
        st.plotly_chart(fig_monthly, width='stretch')
    
    # b. Energy Delivered Analysis
    st.markdown('<h2 class="section-header">⚡ Energy Analysis</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Energy distribution
        fig_energy_dist = px.histogram(
            filtered_df,
            x='energy_consumed_kwh',
            nbins=30,
            title='Energy Consumption Distribution',
            labels={'energy_consumed_kwh': 'Energy (kWh)'},
            color_discrete_sequence=['#636EFA']
        )
        st.plotly_chart(fig_energy_dist, width='stretch')
    
    with col2:
        # Energy by time of day
        energy_by_time = filtered_df.groupby('time_of_day')['energy_consumed_kwh'].sum().reset_index()
        fig_energy_time = px.pie(
            energy_by_time,
            values='energy_consumed_kwh',
            names='time_of_day',
            title='Energy Distribution by Time of Day',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        st.plotly_chart(fig_energy_time, width='stretch')
    
    # c. Charging Duration Analysis
    st.markdown('<h2 class="section-header">⏱️ Charging Duration Analysis</h2>', unsafe_allow_html=True)
    
    duration_col1, duration_col2, duration_col3 = st.columns(3)
    
    with duration_col1:
        avg_duration = filtered_df['charging_duration_hours'].mean()
        st.metric("Average Duration", f"{avg_duration:.2f} hours")
    
    with duration_col2:
        total_duration = filtered_df['charging_duration_hours'].sum()
        st.metric("Total Duration", f"{total_duration:,.2f} hours")
    
    with duration_col3:
        median_duration = filtered_df['charging_duration_hours'].median()
        st.metric("Median Duration", f"{median_duration:.2f} hours")
    
    # Duration charts
    col1, col2 = st.columns(2)
    
    with col1:
        fig_duration_dist = px.box(
            filtered_df,
            y='charging_duration_hours',
            x='time_of_day',
            title='Charging Duration by Time of Day',
            labels={'charging_duration_hours': 'Duration (hours)'},
            color='time_of_day'
        )
        st.plotly_chart(fig_duration_dist, width='stretch')
    
    with col2:
        # Duration over time (use timestamp if single day, otherwise date)
        if len(filtered_df['date'].unique()) == 1:
            # Single day - use hourly aggregation
            duration_over_time = filtered_df.groupby('hour')['charging_duration_hours'].mean().reset_index()
            duration_over_time['hour_label'] = duration_over_time['hour'].astype(str) + ':00'
            
            fig_duration_time = px.line(
                duration_over_time,
                x='hour_label',
                y='charging_duration_hours',
                title='Average Charging Duration by Hour (Today)',
                labels={'charging_duration_hours': 'Avg Duration (hours)', 'hour_label': 'Hour'},
                markers=True
            )
        else:
            # Multiple days - use daily aggregation
            duration_over_time = filtered_df.groupby('date')['charging_duration_hours'].mean().reset_index()
            
            fig_duration_time = px.line(
                duration_over_time,
                x='date',
                y='charging_duration_hours',
                title='Average Charging Duration Over Time',
                labels={'charging_duration_hours': 'Avg Duration (hours)', 'date': 'Date'},
                markers=True
            )

        st.plotly_chart(fig_duration_time, width='stretch')

    
    # d. Distribution by Time of Day
    st.markdown('<h2 class="section-header">🕐 Time of Day Analysis</h2>', unsafe_allow_html=True)
    
    time_analysis = filtered_df.groupby('time_of_day').agg({
        'id': 'count',
        'energy_consumed_kwh': 'sum',
        'charging_cost_eur': 'sum',
        'charging_duration_hours': 'mean'
    }).reset_index()
    time_analysis.columns = ['Time of Day', 'Sessions', 'Total Energy (kWh)', 'Total Cost (€)', 'Avg Duration (hrs)']
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        fig_sessions_time = px.bar(
            time_analysis,
            x='Time of Day',
            y='Sessions',
            title='Charging Sessions by Time of Day',
            color='Sessions',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_sessions_time, width='stretch')
    
    with col2:
        # Hourly distribution
        hourly_dist = filtered_df.groupby('hour').size().reset_index(name='count')
        fig_hourly = px.line(
            hourly_dist,
            x='hour',
            y='count',
            title='Hourly Distribution of Charging Sessions',
            labels={'hour': 'Hour of Day', 'count': 'Number of Sessions'},
            markers=True
        )
        st.plotly_chart(fig_hourly, width='stretch')
    
    st.dataframe(time_analysis, width='stretch', hide_index=True)
    
    # e. Cost Analysis
    st.markdown('<h2 class="section-header">💰 Cost Analysis</h2>', unsafe_allow_html=True)
    
    cost_col1, cost_col2, cost_col3 = st.columns(3)
    
    with cost_col1:
        total_cost = filtered_df['charging_cost_eur'].sum()
        st.metric("Total Cost", f"€{total_cost:,.2f}")
    
    with cost_col2:
        avg_cost = filtered_df['charging_cost_eur'].mean()
        st.metric("Average Cost per Session", f"€{avg_cost:.2f}")
    
    with cost_col3:
        cost_per_kwh = total_cost / filtered_df['energy_consumed_kwh'].sum() if filtered_df['energy_consumed_kwh'].sum() > 0 else 0
        st.metric("Average Cost per kWh", f"€{cost_per_kwh:.3f}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Cost over time (use timestamp if single day, otherwise date)
        if len(filtered_df['date'].unique()) == 1:
            # Single day - use hourly aggregation
            cost_over_time = filtered_df.groupby('hour')['charging_cost_eur'].sum().reset_index()
            cost_over_time['hour_label'] = cost_over_time['hour'].astype(str) + ':00'
            
            fig_cost_time = px.bar(
                cost_over_time,
                x='hour_label',
                y='charging_cost_eur',
                title='Charging Costs by Hour (Today)',
                labels={'charging_cost_eur': 'Cost (€)', 'hour_label': 'Hour'},
                color_discrete_sequence=['#FF6B6B']
            )
        else:
            # Multiple days - use daily aggregation
            cost_over_time = filtered_df.groupby('date')['charging_cost_eur'].sum().reset_index()
            
            fig_cost_time = px.area(
                cost_over_time,
                x='date',
                y='charging_cost_eur',
                title='Daily Charging Costs',
                labels={'charging_cost_eur': 'Cost (€)', 'date': 'Date'},
                color_discrete_sequence=['#FF6B6B']
            )

        st.plotly_chart(fig_cost_time, width='stretch')

    
    with col2:
        # Cost distribution
        fig_cost_dist = px.histogram(
            filtered_df,
            x='charging_cost_eur',
            nbins=30,
            title='Cost Distribution per Session',
            labels={'charging_cost_eur': 'Cost (€)'},
            color_discrete_sequence=['#4ECDC4']
        )
        st.plotly_chart(fig_cost_dist, width='stretch')
    
    # f. Station Utilization Patterns
    st.markdown('<h2 class="section-header">🏢 Station Utilization Patterns</h2>', unsafe_allow_html=True)
    
    # Top stations
    top_stations = filtered_df.groupby('station_id').agg({
        'id': 'count',
        'energy_consumed_kwh': 'sum',
        'charging_cost_eur': 'sum'
    }).reset_index()
    top_stations.columns = ['Station ID', 'Sessions', 'Total Energy (kWh)', 'Total Revenue (€)']
    top_stations = top_stations.sort_values('Sessions', ascending=False).head(20)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_top_stations = px.bar(
            top_stations.head(10),
            x='Sessions',
            y='Station ID',
            orientation='h',
            title='Top 10 Most Used Stations',
            color='Sessions',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_top_stations, width='stretch')
    
    with col2:
        fig_station_energy = px.scatter(
            top_stations,
            x='Sessions',
            y='Total Energy (kWh)',
            size='Total Revenue (€)',
            hover_data=['Station ID'],
            title='Station Performance: Sessions vs Energy',
            color='Total Revenue (€)',
            color_continuous_scale='Plasma'
        )
        st.plotly_chart(fig_station_energy, width='stretch')
    
    # Station utilization table
    st.subheader("📋 Station Utilization Details")
    st.dataframe(
        top_stations.head(20),
        width='stretch',
        hide_index=True,
        column_config={
            "Sessions": st.column_config.NumberColumn(format="%d"),
            "Total Energy (kWh)": st.column_config.NumberColumn(format="%.2f"),
            "Total Revenue (€)": st.column_config.NumberColumn(format="€%.2f")
        }
    )
    
    # Vehicle usage patterns
    st.markdown('<h2 class="section-header">🚗 Vehicle Usage Patterns</h2>', unsafe_allow_html=True)
    
    vehicle_stats = filtered_df.groupby('vehicle_model').agg({
        'id': 'count',
        'energy_consumed_kwh': 'mean',
        'charging_duration_hours': 'mean',
        'charging_cost_eur': 'mean'
    }).reset_index()
    vehicle_stats.columns = ['Vehicle Model', 'Sessions', 'Avg Energy (kWh)', 'Avg Duration (hrs)', 'Avg Cost (€)']
    vehicle_stats = vehicle_stats.sort_values('Sessions', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_vehicle_sessions = px.pie(
            vehicle_stats.head(10),
            values='Sessions',
            names='Vehicle Model',
            title='Sessions by Vehicle Model (Top 10)',
            hole=0.4
        )
        st.plotly_chart(fig_vehicle_sessions, width='stretch')
    
    with col2:
        fig_vehicle_energy = px.bar(
            vehicle_stats.head(10),
            x='Vehicle Model',
            y='Avg Energy (kWh)',
            title='Average Energy Consumption by Vehicle Model',
            color='Avg Energy (kWh)',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_vehicle_energy, width='stretch')
    
    # Day of week patterns
    st.markdown('<h2 class="section-header">📅 Weekly Patterns</h2>', unsafe_allow_html=True)
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_stats = filtered_df.groupby('day_of_week').agg({
        'id': 'count',
        'energy_consumed_kwh': 'sum',
        'charging_cost_eur': 'sum'
    }).reset_index()
    day_stats['day_of_week'] = pd.Categorical(day_stats['day_of_week'], categories=day_order, ordered=True)
    day_stats = day_stats.sort_values('day_of_week')
    
    fig_weekly_pattern = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Sessions by Day of Week', 'Energy Consumption by Day')
    )
    
    fig_weekly_pattern.add_trace(
        go.Bar(x=day_stats['day_of_week'], y=day_stats['id'], name='Sessions',
               marker_color='lightcoral'),
        row=1, col=1
    )
    
    fig_weekly_pattern.add_trace(
        go.Bar(x=day_stats['day_of_week'], y=day_stats['energy_consumed_kwh'], name='Energy (kWh)',
               marker_color='lightseagreen'),
        row=1, col=2
    )
    
    fig_weekly_pattern.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_weekly_pattern, width='stretch')
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: gray;'>"
        f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Total Records: {len(df):,} | Filtered Records: {len(filtered_df):,}"
        f"</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
