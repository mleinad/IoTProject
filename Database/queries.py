import mysql.connector
import pandas as pd


def execute_query(connection, query, params=None):
    """
    Execute a SELECT query and return results as a list of tuples.
    
    Args:
        connection: MySQL connection object
        query: SQL query string
        params: Optional tuple of parameters for parameterized queries
    
    Returns:
        List of tuples containing query results
    """
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except mysql.connector.Error as e:
        print(f"Error executing query: {e}")
        return None


def query_to_dataframe(connection, query, params=None):
    """
    Execute a query and return results as a pandas DataFrame.
    
    Args:
        connection: MySQL connection object
        query: SQL query string
        params: Optional tuple of parameters
    
    Returns:
        pandas DataFrame with query results
    """
    try:
        if params:
            df = pd.read_sql(query, connection, params=params)
        else:
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None

# ========================================
# DAILY/WEEKLY/MONTHLY TRENDS
# ========================================

def get_daily_trends(connection):
    """Get daily aggregated charging statistics."""
    query = """
        SELECT 
            DATE(charging_start_time) as date,
            COUNT(*) as total_sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh,
            ROUND(SUM(charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT charging_station_id) as stations_used
        FROM ev_charging_data
        GROUP BY DATE(charging_start_time)
        ORDER BY DATE(charging_start_time)
    """
    return execute_query(connection, query)


def get_weekly_trends(connection):
    """Get weekly aggregated charging statistics."""
    query = """
        SELECT 
            YEAR(charging_start_time) as year,
            WEEK(charging_start_time, 1) as week_number,
            CONCAT(YEAR(charging_start_time), '-W', LPAD(WEEK(charging_start_time, 1), 2, '0')) as year_week,
            MIN(DATE(charging_start_time)) as week_start,
            MAX(DATE(charging_start_time)) as week_end,
            COUNT(*) as total_sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh,
            ROUND(SUM(charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            COUNT(DISTINCT user_id) as unique_users
        FROM ev_charging_data
        GROUP BY YEAR(charging_start_time), WEEK(charging_start_time, 1)
        ORDER BY YEAR(charging_start_time), WEEK(charging_start_time, 1)
    """
    return execute_query(connection, query)


def get_monthly_trends(connection):
    """Get monthly aggregated charging statistics."""
    query = """
        SELECT 
            CONCAT(YEAR(charging_start_time), '-', LPAD(MONTH(charging_start_time), 2, '0')) as month,
            YEAR(charging_start_time) as year,
            MONTH(charging_start_time) as month_number,
            DATE_FORMAT(MIN(charging_start_time), '%M %Y') as month_name,
            COUNT(*) as total_sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh,
            ROUND(SUM(charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT charging_station_id) as stations_used
        FROM ev_charging_data
        GROUP BY YEAR(charging_start_time), MONTH(charging_start_time)
        ORDER BY YEAR(charging_start_time), MONTH(charging_start_time)
    """
    return execute_query(connection, query)


def get_trend_comparison(connection):
    """Compare daily, weekly, and monthly trends side by side."""
    query = """
        SELECT 
            'Daily Average' as period_type,
            ROUND(AVG(daily_sessions), 2) as avg_sessions,
            ROUND(AVG(daily_energy), 2) as avg_energy_kwh,
            ROUND(AVG(daily_revenue), 2) as avg_revenue
        FROM (
            SELECT 
                DATE(charging_start_time) as date,
                COUNT(*) as daily_sessions,
                SUM(energy_consumed_kwh) as daily_energy,
                SUM(charging_cost_eur) as daily_revenue
            FROM ev_charging_data
            GROUP BY DATE(charging_start_time)
        ) daily_stats
        
        UNION ALL
        
        SELECT 
            'Weekly Average' as period_type,
            ROUND(AVG(weekly_sessions), 2) as avg_sessions,
            ROUND(AVG(weekly_energy), 2) as avg_energy_kwh,
            ROUND(AVG(weekly_revenue), 2) as avg_revenue
        FROM (
            SELECT 
                YEAR(charging_start_time) as year,
                WEEK(charging_start_time, 1) as week,
                COUNT(*) as weekly_sessions,
                SUM(energy_consumed_kwh) as weekly_energy,
                SUM(charging_cost_eur) as weekly_revenue
            FROM ev_charging_data
            GROUP BY YEAR(charging_start_time), WEEK(charging_start_time, 1)
        ) weekly_stats
        
        UNION ALL
        
        SELECT 
            'Monthly Average' as period_type,
            ROUND(AVG(monthly_sessions), 2) as avg_sessions,
            ROUND(AVG(monthly_energy), 2) as avg_energy_kwh,
            ROUND(AVG(monthly_revenue), 2) as avg_revenue
        FROM (
            SELECT 
                YEAR(charging_start_time) as year,
                MONTH(charging_start_time) as month,
                COUNT(*) as monthly_sessions,
                SUM(energy_consumed_kwh) as monthly_energy,
                SUM(charging_cost_eur) as monthly_revenue
            FROM ev_charging_data
            GROUP BY YEAR(charging_start_time), MONTH(charging_start_time)
        ) monthly_stats
    """
    return execute_query(connection, query)


def get_hourly_distribution(connection):
    """Analyze charging distribution by hour of day."""
    query = """
        SELECT 
            HOUR(charging_start_time) as hour,
            COUNT(*) as session_count,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration
        FROM ev_charging_data
        GROUP BY HOUR(charging_start_time)
        ORDER BY HOUR(charging_start_time)
    """
    return execute_query(connection, query)


# ========================================
# ENERGY DELIVERED ANALYSIS
# ========================================

def get_total_energy_delivered(connection):
    """
    Get overall energy delivery statistics.
    """
    query = """
        SELECT 
            COUNT(*) as total_sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_delivered_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(MIN(energy_consumed_kwh), 2) as min_energy_kwh,
            ROUND(MAX(energy_consumed_kwh), 2) as max_energy_kwh,
            ROUND(STD(energy_consumed_kwh), 2) as energy_std_dev
        FROM ev_charging_data
        WHERE energy_consumed_kwh IS NOT NULL
    """
    return execute_query(connection, query)


def get_energy_by_vehicle_model(connection):
    """
    Energy delivered breakdown by vehicle model.
    """
    query = """
        SELECT 
            vehicle_model,
            COUNT(*) as sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(AVG(battery_capacity_kwh), 2) as avg_battery_capacity,
            ROUND((AVG(energy_consumed_kwh) / AVG(battery_capacity_kwh)) * 100, 2) as avg_battery_pct_charged
        FROM ev_charging_data
        WHERE energy_consumed_kwh IS NOT NULL 
              AND battery_capacity_kwh IS NOT NULL
        GROUP BY vehicle_model
        ORDER BY total_energy_kwh DESC
    """
    return execute_query(connection, query)


def get_energy_by_station(connection, limit=50):
    """
    Energy delivered breakdown by charging station.
    """
    query = """
        SELECT 
            charging_station_id,
            COUNT(*) as sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_delivered_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(AVG(charging_rate_kw), 2) as avg_charging_rate_kw
        FROM ev_charging_data
        WHERE energy_consumed_kwh IS NOT NULL
        GROUP BY charging_station_id
        ORDER BY total_energy_delivered_kwh DESC
        LIMIT %s
    """
    return execute_query(connection, query, (limit,))

def get_daily_energy_delivered(connection):
    """Daily energy delivery trends."""
    query = """
        SELECT 
            DATE(charging_start_time) as date,
            COUNT(*) as sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh,
            ROUND(SUM(energy_consumed_kwh) / NULLIF(SUM(charging_duration_hours), 0), 2) as avg_power_kw
        FROM ev_charging_data
        WHERE energy_consumed_kwh IS NOT NULL 
              AND charging_duration_hours > 0
        GROUP BY DATE(charging_start_time)
        ORDER BY DATE(charging_start_time)
    """
    return execute_query(connection, query)

# ========================================
# CHARGING DURATION ANALYSIS
# ========================================

def get_duration_statistics(connection):
    """
    Overall charging duration statistics.
    """
    query = """
        SELECT 
            COUNT(*) as total_sessions,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            ROUND(MIN(charging_duration_hours), 2) as min_duration_hours,
            ROUND(MAX(charging_duration_hours), 2) as max_duration_hours,
            ROUND(STD(charging_duration_hours), 2) as duration_std_dev,
            ROUND(AVG(charging_duration_hours) * 60, 2) as avg_duration_minutes
        FROM ev_charging_data
        WHERE charging_duration_hours IS NOT NULL
    """
    return execute_query(connection, query)


def get_duration_distribution(connection):
    """
    Distribution of charging durations by time ranges.
    """
    query = """
        SELECT 
            CASE 
                WHEN charging_duration_hours < 0.5 THEN '< 30 min'
                WHEN charging_duration_hours BETWEEN 0.5 AND 1 THEN '30 min - 1 hour'
                WHEN charging_duration_hours BETWEEN 1 AND 2 THEN '1-2 hours'
                WHEN charging_duration_hours BETWEEN 2 AND 4 THEN '2-4 hours'
                WHEN charging_duration_hours BETWEEN 4 AND 6 THEN '4-6 hours'
                ELSE '> 6 hours'
            END as duration_range,
            COUNT(*) as session_count,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ev_charging_data)), 2) as percentage
        FROM ev_charging_data
        WHERE charging_duration_hours IS NOT NULL
        GROUP BY duration_range
        ORDER BY 
            CASE duration_range
                WHEN '< 30 min' THEN 1
                WHEN '30 min - 1 hour' THEN 2
                WHEN '1-2 hours' THEN 3
                WHEN '2-4 hours' THEN 4
                WHEN '4-6 hours' THEN 5
                ELSE 6
            END
    """
    return execute_query(connection, query)


def get_duration_by_vehicle_model(connection):
    """
    Average charging duration by vehicle model.
    """
    query = """
        SELECT 
            vehicle_model,
            COUNT(*) as sessions,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            ROUND(AVG(charging_duration_hours) * 60, 2) as avg_duration_minutes,
            ROUND(MIN(charging_duration_hours), 2) as min_duration_hours,
            ROUND(MAX(charging_duration_hours), 2) as max_duration_hours
        FROM ev_charging_data
        WHERE charging_duration_hours IS NOT NULL
        GROUP BY vehicle_model
        ORDER BY avg_duration_hours DESC
    """
    return execute_query(connection, query)


def get_duration_by_time_of_day(connection):
    """
    Charging duration patterns by time of day.
    """
    query = """
        SELECT 
            time_of_day,
            COUNT(*) as sessions,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            ROUND(AVG(charging_duration_hours) * 60, 2) as avg_duration_minutes,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh
        FROM ev_charging_data
        WHERE time_of_day IS NOT NULL 
              AND charging_duration_hours IS NOT NULL
        GROUP BY time_of_day
        ORDER BY 
            CASE time_of_day
                WHEN 'Morning' THEN 1
                WHEN 'Afternoon' THEN 2
                WHEN 'Evening' THEN 3
                WHEN 'Night' THEN 4
            END
    """
    return execute_query(connection, query)


# ========================================
# TIME-OF-DAY DISTRIBUTIONS
# ========================================

def get_time_of_day_distribution(connection):
    """
    Distribution of charging sessions by time of day.
    """
    query = """
        SELECT 
            time_of_day,
            COUNT(*) as session_count,
            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ev_charging_data WHERE time_of_day IS NOT NULL)), 2) as percentage,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours
        FROM ev_charging_data
        WHERE time_of_day IS NOT NULL
        GROUP BY time_of_day
        ORDER BY 
            CASE time_of_day
                WHEN 'Morning' THEN 1
                WHEN 'Afternoon' THEN 2
                WHEN 'Evening' THEN 3
                WHEN 'Night' THEN 4
            END
    """
    return execute_query(connection, query)

def get_hourly_distribution(connection):
    """
    Analyze charging distribution by hour of day.
    """
    query = """
        SELECT 
            HOUR(charging_start_time) as hour,
            COUNT(*) as session_count,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost
        FROM ev_charging_data
        GROUP BY HOUR(charging_start_time)
        ORDER BY hour
    """
    return execute_query(connection, query)

def get_day_of_week_distribution(connection):
    """
    Distribution of charging sessions by day of week.
    """
    query = """
        SELECT 
            day_of_week,
            COUNT(*) as session_count,
            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ev_charging_data WHERE day_of_week IS NOT NULL)), 2) as percentage,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy
        FROM ev_charging_data
        WHERE day_of_week IS NOT NULL
        GROUP BY day_of_week
        ORDER BY 
            CASE day_of_week
                WHEN 'Monday' THEN 1
                WHEN 'Tuesday' THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4
                WHEN 'Friday' THEN 5
                WHEN 'Saturday' THEN 6
                WHEN 'Sunday' THEN 7
            END
    """
    return execute_query(connection, query)


def get_weekend_vs_weekday(connection):
    """
    Compare weekend vs weekday charging patterns.
    """
    query = """
        SELECT 
            CASE 
                WHEN day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
                ELSE 'Weekday'
            END as period_type,
            COUNT(*) as session_count,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration
        FROM ev_charging_data
        WHERE day_of_week IS NOT NULL
        GROUP BY period_type
    """
    return execute_query(connection, query)


# ========================================
# COST OVER TIME ANALYSIS
# ========================================

def get_daily_cost_trends(connection):
    """Daily cost trends over time."""
    query = """
        SELECT 
            DATE(charging_start_time) as date,
            COUNT(*) as sessions,
            ROUND(SUM(charging_cost_eur), 2) as total_cost,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost_per_session,
            ROUND(MIN(charging_cost_eur), 2) as min_cost,
            ROUND(MAX(charging_cost_eur), 2) as max_cost
        FROM ev_charging_data
        WHERE charging_cost_eur IS NOT NULL
        GROUP BY DATE(charging_start_time)
        ORDER BY DATE(charging_start_time)
    """
    return execute_query(connection, query)


def get_weekly_cost_trends(connection):
    """Weekly cost trends over time."""
    query = """
        SELECT 
            CONCAT(YEAR(charging_start_time), '-W', LPAD(WEEK(charging_start_time, 1), 2, '0')) as year_week,
            MIN(DATE(charging_start_time)) as week_start,
            COUNT(*) as sessions,
            ROUND(SUM(charging_cost_eur), 2) as total_cost,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost_per_session
        FROM ev_charging_data
        WHERE charging_cost_eur IS NOT NULL
        GROUP BY YEAR(charging_start_time), WEEK(charging_start_time, 1)
        ORDER BY YEAR(charging_start_time), WEEK(charging_start_time, 1)
    """
    return execute_query(connection, query)


def get_monthly_cost_trends(connection):
    """Monthly cost trends over time."""
    query = """
        SELECT 
            CONCAT(YEAR(charging_start_time), '-', LPAD(MONTH(charging_start_time), 2, '0')) as month,
            DATE_FORMAT(MIN(charging_start_time), '%M %Y') as month_name,
            COUNT(*) as sessions,
            ROUND(SUM(charging_cost_eur), 2) as total_cost,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost_per_session,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy,
            ROUND(SUM(charging_cost_eur) / NULLIF(SUM(energy_consumed_kwh), 0), 2) as cost_per_kwh
        FROM ev_charging_data
        WHERE charging_cost_eur IS NOT NULL 
              AND energy_consumed_kwh > 0
        GROUP BY YEAR(charging_start_time), MONTH(charging_start_time)
        ORDER BY YEAR(charging_start_time), MONTH(charging_start_time)
    """
    return execute_query(connection, query)


def get_cost_trends_by_time_of_day(connection):
    """Cost trends by time of day over time periods."""
    query = """
        SELECT 
            DATE(charging_start_time) as date,
            time_of_day,
            COUNT(*) as sessions,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost
        FROM ev_charging_data
        WHERE time_of_day IS NOT NULL 
              AND charging_cost_eur IS NOT NULL
        GROUP BY DATE(charging_start_time), time_of_day
        ORDER BY DATE(charging_start_time), time_of_day
    """
    return execute_query(connection, query)

def get_cost_statistics(connection):
    """
    Overall cost statistics across all charging sessions.
    """
    query = """
        SELECT 
            COUNT(*) as total_sessions,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(MIN(charging_cost_eur), 2) as min_cost,
            ROUND(MAX(charging_cost_eur), 2) as max_cost,
            ROUND(SUM(charging_cost_eur), 2) as total_revenue,
            ROUND(STD(charging_cost_eur), 2) as cost_std_dev
        FROM ev_charging_data
        WHERE charging_cost_eur IS NOT NULL
    """
    return execute_query(connection, query)

# ========================================
# USAGE PER USER
# ========================================

def get_usage_per_user(connection):
    """
    Comprehensive usage statistics per user.
    """
    query = """
        SELECT 
            user_id,
            vehicle_model,
            COUNT(*) as total_sessions,
            ROUND(SUM(charging_cost_eur), 2) as total_spent,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost_per_session,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            COUNT(DISTINCT charging_station_id) as stations_used,
            COUNT(DISTINCT DATE(charging_start_time)) as days_active
        FROM ev_charging_data
        GROUP BY user_id, vehicle_model
        ORDER BY total_sessions DESC
    """
    return execute_query(connection, query)


def get_user_charging_frequency(connection):
    """
    User charging frequency analysis.
    """
    query = """
        SELECT 
            user_id,
            COUNT(*) as total_sessions,
            MIN(DATE(charging_start_time)) as first_charge,
            MAX(DATE(charging_start_time)) as last_charge,
            DATEDIFF(MAX(charging_start_time), MIN(charging_start_time)) as days_span,
            ROUND(COUNT(*) / NULLIF(DATEDIFF(MAX(charging_start_time), MIN(charging_start_time)), 0), 2) as sessions_per_day
        FROM ev_charging_data
        GROUP BY user_id
        HAVING days_span > 0
        ORDER BY sessions_per_day DESC
    """
    return execute_query(connection, query)


def get_user_daily_usage(connection, user_id=None):
    """
    Daily usage breakdown for users.
    """
    query = """
        SELECT 
            user_id,
            DATE(charging_start_time) as date,
            COUNT(*) as sessions_that_day,
            ROUND(SUM(charging_cost_eur), 2) as daily_cost,
            ROUND(SUM(energy_consumed_kwh), 2) as daily_energy
        FROM ev_charging_data
    """
    
    if user_id:
        query += " WHERE user_id = %s"
        query += " GROUP BY user_id, DATE(charging_start_time) ORDER BY date"
        return execute_query(connection, query, (user_id,))
    else:
        query += " GROUP BY user_id, DATE(charging_start_time) ORDER BY user_id, date"
        return execute_query(connection, query)


def get_top_users_ranking(connection, limit=50):
    """
    Rank top users by multiple metrics.
    """
    query = """
        SELECT 
            user_id,
            vehicle_model,
            COUNT(*) as sessions,
            ROUND(SUM(charging_cost_eur), 2) as total_spent,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy,
            ROUND(SUM(charging_duration_hours), 2) as total_hours,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            RANK() OVER (ORDER BY COUNT(*) DESC) as rank_by_frequency,
            RANK() OVER (ORDER BY SUM(charging_cost_eur) DESC) as rank_by_spending,
            RANK() OVER (ORDER BY SUM(energy_consumed_kwh) DESC) as rank_by_energy
        FROM ev_charging_data
        GROUP BY user_id, vehicle_model
        ORDER BY sessions DESC
        LIMIT %s
    """
    return execute_query(connection, query, (limit,))


# ========================================
# USAGE PER STATION
# ========================================

def get_usage_per_station(connection):
    """
    Comprehensive usage statistics per charging station with location data.
    """
    query = """
        SELECT 
            c.charging_station_id,
            s.distrito,
            s.concelho,
            s.freguesia,
            ROUND(s.potencia_maxima_kw, 2) as station_power_kw,
            s.pontos_ligacao as connection_points,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT c.user_id) as unique_users,
            ROUND(SUM(c.charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(c.charging_cost_eur), 2) as avg_revenue_per_session,
            ROUND(SUM(c.energy_consumed_kwh), 2) as total_energy_delivered,
            ROUND(AVG(c.energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(AVG(c.charging_duration_hours), 2) as avg_duration_hours,
            ROUND(AVG(c.charging_rate_kw), 2) as avg_charging_rate_kw,
            COUNT(DISTINCT DATE(c.charging_start_time)) as days_active
        FROM ev_charging_data c
        LEFT JOIN ev_stations s ON c.charging_station_id = s.station_id
        GROUP BY c.charging_station_id, s.distrito, s.concelho, s.freguesia,
                 s.potencia_maxima_kw, s.pontos_ligacao
        ORDER BY total_sessions DESC
    """
    return execute_query(connection, query)



def get_station_daily_usage(connection, station_id=None):
    """
    Daily usage breakdown for stations.
    """
    query = """
        SELECT 
            charging_station_id,
            DATE(charging_start_time) as date,
            COUNT(*) as sessions_that_day,
            COUNT(DISTINCT user_id) as unique_users,
            ROUND(SUM(charging_cost_eur), 2) as daily_revenue,
            ROUND(SUM(energy_consumed_kwh), 2) as daily_energy
        FROM ev_charging_data
    """
    
    if station_id:
        query += " WHERE charging_station_id = %s"
        query += " GROUP BY charging_station_id, DATE(charging_start_time) ORDER BY date"
        return execute_query(connection, query, (station_id,))
    else:
        query += " GROUP BY charging_station_id, DATE(charging_start_time) ORDER BY charging_station_id, date"
        return execute_query(connection, query)


def get_station_utilization_rate(connection):
    """
    Calculate station utilization rate (sessions per day).
    """
    query = """
        SELECT 
            charging_station_id,
            COUNT(*) as total_sessions,
            MIN(DATE(charging_start_time)) as first_use,
            MAX(DATE(charging_start_time)) as last_use,
            DATEDIFF(MAX(charging_start_time), MIN(charging_start_time)) + 1 as days_in_operation,
            ROUND(COUNT(*) / (DATEDIFF(MAX(charging_start_time), MIN(charging_start_time)) + 1), 2) as sessions_per_day,
            ROUND(SUM(charging_duration_hours) / (DATEDIFF(MAX(charging_start_time), MIN(charging_start_time)) + 1), 2) as hours_used_per_day
        FROM ev_charging_data
        GROUP BY charging_station_id
        ORDER BY sessions_per_day DESC
    """
    return execute_query(connection, query)


def get_top_stations_ranking(connection, limit=50):
    """
    Rank top stations by multiple metrics.
    """
    query = """
        SELECT 
            charging_station_id,
            COUNT(*) as sessions,
            ROUND(SUM(charging_cost_eur), 2) as revenue,
            ROUND(SUM(energy_consumed_kwh), 2) as energy_delivered,
            COUNT(DISTINCT user_id) as unique_users,
            RANK() OVER (ORDER BY COUNT(*) DESC) as rank_by_usage,
            RANK() OVER (ORDER BY SUM(charging_cost_eur) DESC) as rank_by_revenue,
            RANK() OVER (ORDER BY COUNT(DISTINCT user_id) DESC) as rank_by_user_diversity
        FROM ev_charging_data
        GROUP BY charging_station_id
        ORDER BY sessions DESC
        LIMIT %s
    """
    return execute_query(connection, query, (limit,))


def get_station_peak_hours(connection, station_id):
    """
    Find peak usage hours for a specific station.
    """
    query = """
        SELECT 
            HOUR(charging_start_time) as hour,
            COUNT(*) as sessions,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration
        FROM ev_charging_data
        WHERE charging_station_id = %s
        GROUP BY HOUR(charging_start_time)
        ORDER BY sessions DESC
    """
    return execute_query(connection, query, (station_id,))

# ========================================
# STATION LOCATION & GEOGRAPHIC QUERIES
# ========================================

def get_stations_by_distrito(connection):
    """
    Get station count and statistics by distrito.
    """
    query = """
        SELECT 
            distrito,
            COUNT(*) as total_stations,
            ROUND(AVG(potencia_maxima_kw), 2) as avg_power_kw,
            ROUND(MIN(potencia_maxima_kw), 2) as min_power_kw,
            ROUND(MAX(potencia_maxima_kw), 2) as max_power_kw,
            SUM(pontos_ligacao) as total_connection_points
        FROM ev_stations
        GROUP BY distrito
        ORDER BY total_stations DESC
    """
    return execute_query(connection, query)


def get_stations_by_concelho(connection, distrito=None):
    """
    Get station count by concelho, optionally filtered by distrito.
    """
    query = """
        SELECT 
            distrito,
            concelho,
            COUNT(*) as total_stations,
            ROUND(AVG(potencia_maxima_kw), 2) as avg_power_kw,
            SUM(pontos_ligacao) as total_connection_points
        FROM ev_stations
    """
    
    if distrito:
        query += " WHERE distrito = %s"
        query += " GROUP BY distrito, concelho ORDER BY total_stations DESC"
        return execute_query(connection, query, (distrito,))
    else:
        query += " GROUP BY distrito, concelho ORDER BY total_stations DESC LIMIT 50"
        return execute_query(connection, query)


def get_stations_by_freguesia(connection, concelho=None):
    """
    Get station count by freguesia, optionally filtered by concelho.
    """
    query = """
        SELECT 
            distrito,
            concelho,
            freguesia,
            COUNT(*) as total_stations,
            ROUND(AVG(potencia_maxima_kw), 2) as avg_power_kw
        FROM ev_stations
    """
    
    if concelho:
        query += " WHERE concelho = %s"
        query += " GROUP BY distrito, concelho, freguesia ORDER BY total_stations DESC"
        return execute_query(connection, query, (concelho,))
    else:
        query += " GROUP BY distrito, concelho, freguesia ORDER BY total_stations DESC LIMIT 50"
        return execute_query(connection, query)


def get_power_distribution(connection):
    """
    Analyze distribution of station power capacities.
    """
    query = """
        SELECT 
            CASE 
                WHEN potencia_maxima_kw < 50 THEN '< 50 kW (Slow)'
                WHEN potencia_maxima_kw BETWEEN 50 AND 100 THEN '50-100 kW (Medium)'
                WHEN potencia_maxima_kw BETWEEN 100 AND 150 THEN '100-150 kW (Fast)'
                WHEN potencia_maxima_kw BETWEEN 150 AND 300 THEN '150-300 kW (Very Fast)'
                ELSE '> 300 kW (Ultra Fast)'
            END as power_category,
            COUNT(*) as station_count,
            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ev_stations)), 2) as percentage,
            ROUND(AVG(potencia_maxima_kw), 2) as avg_power,
            SUM(pontos_ligacao) as total_connection_points
        FROM ev_stations
        WHERE potencia_maxima_kw IS NOT NULL
        GROUP BY power_category
        ORDER BY 
            CASE power_category
                WHEN '< 50 kW (Slow)' THEN 1
                WHEN '50-100 kW (Medium)' THEN 2
                WHEN '100-150 kW (Fast)' THEN 3
                WHEN '150-300 kW (Very Fast)' THEN 4
                ELSE 5
            END
    """
    return execute_query(connection, query)


def get_high_power_stations(connection, min_power=150, limit=50):
    """
    Find high-power charging stations (fast chargers).
    """
    query = """
        SELECT 
            station_id,
            distrito,
            concelho,
            freguesia,
            ROUND(potencia_maxima_kw, 2) as power_kw,
            pontos_ligacao as connection_points,
            ROUND(latitude, 5) as lat,
            ROUND(longitude, 5) as lon
        FROM ev_stations
        WHERE potencia_maxima_kw >= %s
        ORDER BY potencia_maxima_kw DESC
        LIMIT %s
    """
    return execute_query(connection, query, (min_power, limit))


def get_connection_points_analysis(connection):
    """
    Analyze distribution of connection points per station.
    """
    query = """
        SELECT 
            pontos_ligacao as connection_points,
            COUNT(*) as station_count,
            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ev_stations)), 2) as percentage,
            ROUND(AVG(potencia_maxima_kw), 2) as avg_power_kw
        FROM ev_stations
        WHERE pontos_ligacao IS NOT NULL
        GROUP BY pontos_ligacao
        ORDER BY pontos_ligacao
    """
    return execute_query(connection, query)


# ========================================
# CHARGING DATA + STATION LOCATION JOINS
# ========================================

def get_charging_sessions_with_location(connection, limit=100):
    """
    Get charging sessions with station location information.
    """
    query = """
        SELECT 
            c.charging_station_id,
            s.distrito,
            s.concelho,
            s.freguesia,
            COUNT(*) as sessions,
            ROUND(AVG(c.energy_consumed_kwh), 2) as avg_energy,
            ROUND(AVG(c.charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(c.charging_duration_hours), 2) as avg_duration,
            ROUND(s.potencia_maxima_kw, 2) as station_power_kw,
            s.pontos_ligacao as connection_points
        FROM ev_charging_data c
        LEFT JOIN ev_stations s ON c.charging_station_id = s.station_id
        GROUP BY c.charging_station_id, s.distrito, s.concelho, s.freguesia, 
                 s.potencia_maxima_kw, s.pontos_ligacao
        ORDER BY sessions DESC
        LIMIT %s
    """
    return execute_query(connection, query, (limit,))


def get_usage_by_distrito(connection):
    """
    Analyze charging usage patterns by distrito.
    """
    query = """
        SELECT 
            s.distrito,
            COUNT(DISTINCT c.charging_station_id) as stations_used,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT c.user_id) as unique_users,
            ROUND(SUM(c.energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(c.energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(SUM(c.charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(c.charging_cost_eur), 2) as avg_cost_per_session
        FROM ev_charging_data c
        LEFT JOIN ev_stations s ON c.charging_station_id = s.station_id
        WHERE s.distrito IS NOT NULL
        GROUP BY s.distrito
        ORDER BY total_sessions DESC
    """
    return execute_query(connection, query)


def get_usage_by_concelho(connection, distrito=None):
    """
    Analyze charging usage patterns by concelho.
    """
    query = """
        SELECT 
            s.distrito,
            s.concelho,
            COUNT(DISTINCT c.charging_station_id) as stations_used,
            COUNT(*) as total_sessions,
            ROUND(SUM(c.energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(SUM(c.charging_cost_eur), 2) as total_revenue
        FROM ev_charging_data c
        LEFT JOIN ev_stations s ON c.charging_station_id = s.station_id
        WHERE s.concelho IS NOT NULL
    """
    
    if distrito:
        query += " AND s.distrito = %s"
        query += " GROUP BY s.distrito, s.concelho ORDER BY total_sessions DESC"
        return execute_query(connection, query, (distrito,))
    else:
        query += " GROUP BY s.distrito, s.concelho ORDER BY total_sessions DESC LIMIT 30"
        return execute_query(connection, query)


def get_stations_with_no_usage(connection):
    """
    Find stations that exist in database but have no charging sessions.
    """
    query = """
        SELECT 
            s.station_id,
            s.distrito,
            s.concelho,
            s.freguesia,
            ROUND(s.potencia_maxima_kw, 2) as power_kw,
            s.pontos_ligacao
        FROM ev_stations s
        LEFT JOIN ev_charging_data c ON s.station_id = c.charging_station_id
        WHERE c.charging_station_id IS NULL
        ORDER BY s.potencia_maxima_kw DESC
        LIMIT 100
    """
    return execute_query(connection, query)


def get_power_vs_usage_analysis(connection):
    """
    Compare station power capacity with actual usage.
    """
    query = """
        SELECT 
            CASE 
                WHEN s.potencia_maxima_kw < 50 THEN '< 50 kW'
                WHEN s.potencia_maxima_kw BETWEEN 50 AND 100 THEN '50-100 kW'
                WHEN s.potencia_maxima_kw BETWEEN 100 AND 150 THEN '100-150 kW'
                ELSE '> 150 kW'
            END as power_range,
            COUNT(DISTINCT s.station_id) as total_stations,
            COUNT(DISTINCT c.charging_station_id) as stations_with_usage,
            COUNT(c.id) as total_sessions,
            ROUND(AVG(c.energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(AVG(c.charging_duration_hours), 2) as avg_duration_hours,
            ROUND(AVG(s.potencia_maxima_kw), 2) as avg_station_power
        FROM ev_stations s
        LEFT JOIN ev_charging_data c ON s.station_id = c.charging_station_id
        WHERE s.potencia_maxima_kw IS NOT NULL
        GROUP BY power_range
        ORDER BY avg_station_power
    """
    return execute_query(connection, query)


def get_geographic_coverage_stats(connection):
    """
    Get comprehensive geographic coverage statistics.
    """
    query = """
        SELECT 
            'Total Coverage' as metric,
            COUNT(DISTINCT distrito) as distritos,
            COUNT(DISTINCT concelho) as concelhos,
            COUNT(DISTINCT freguesia) as freguesias,
            COUNT(*) as total_stations,
            ROUND(AVG(potencia_maxima_kw), 2) as avg_power
        FROM ev_stations
        
        UNION ALL
        
        SELECT 
            'With Charging Sessions' as metric,
            COUNT(DISTINCT s.distrito) as distritos,
            COUNT(DISTINCT s.concelho) as concelhos,
            COUNT(DISTINCT s.freguesia) as freguesias,
            COUNT(DISTINCT s.station_id) as total_stations,
            ROUND(AVG(s.potencia_maxima_kw), 2) as avg_power
        FROM ev_stations s
        INNER JOIN ev_charging_data c ON s.station_id = c.charging_station_id
    """
    return execute_query(connection, query)


def get_stations_near_coordinates(connection, latitude, longitude, max_distance_km=10):
    """
    Find stations near specific coordinates using Haversine formula.
    Note: This is an approximation. For production, use MySQL spatial functions.
    """
    query = """
        SELECT 
            station_id,
            distrito,
            concelho,
            freguesia,
            ROUND(potencia_maxima_kw, 2) as power_kw,
            pontos_ligacao,
            ROUND(latitude, 6) as lat,
            ROUND(longitude, 6) as lon,
            ROUND(
                6371 * 2 * ASIN(SQRT(
                    POWER(SIN((RADIANS(latitude) - RADIANS(%s)) / 2), 2) +
                    COS(RADIANS(%s)) * COS(RADIANS(latitude)) *
                    POWER(SIN((RADIANS(longitude) - RADIANS(%s)) / 2), 2)
                )),
                2
            ) as distance_km
        FROM ev_stations
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        HAVING distance_km <= %s
        ORDER BY distance_km
        LIMIT 50
    """
    return execute_query(connection, query, (latitude, latitude, longitude, max_distance_km))


def get_station_density_by_area(connection):
    """
    Calculate station density (stations per concelho).
    """
    query = """
        SELECT 
            distrito,
            concelho,
            COUNT(*) as station_count,
            SUM(pontos_ligacao) as total_connection_points,
            ROUND(AVG(potencia_maxima_kw), 2) as avg_power,
            ROUND(COUNT(*) / (SELECT COUNT(DISTINCT concelho) 
                              FROM ev_stations s2 
                              WHERE s2.distrito = s1.distrito), 2) as stations_per_concelho_in_distrito
        FROM ev_stations s1
        GROUP BY distrito, concelho
        HAVING station_count >= 10
        ORDER BY station_count DESC
        LIMIT 30
    """
    return execute_query(connection, query)


def get_top_locations_by_revenue(connection, limit=20):
    """
    Find top revenue-generating locations (distrito/concelho combinations).
    """
    query = """
        SELECT 
            s.distrito,
            s.concelho,
            COUNT(DISTINCT c.charging_station_id) as stations,
            COUNT(*) as sessions,
            ROUND(SUM(c.charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(c.charging_cost_eur), 2) as avg_cost_per_session,
            ROUND(SUM(c.energy_consumed_kwh), 2) as total_energy_kwh
        FROM ev_charging_data c
        INNER JOIN ev_stations s ON c.charging_station_id = s.station_id
        WHERE s.distrito IS NOT NULL AND s.concelho IS NOT NULL
        GROUP BY s.distrito, s.concelho
        ORDER BY total_revenue DESC
        LIMIT %s
    """
    return execute_query(connection, query, (limit,))
