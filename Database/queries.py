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
    """
    Get daily aggregated charging statistics.
    """
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
        ORDER BY date
    """
    return execute_query(connection, query)


def get_weekly_trends(connection):
    """
    Get weekly aggregated charging statistics.
    """
    query = """
        SELECT 
            YEAR(charging_start_time) as year,
            WEEK(charging_start_time) as week_number,
            DATE_FORMAT(charging_start_time, '%Y-W%u') as year_week,
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
        GROUP BY YEAR(charging_start_time), WEEK(charging_start_time)
        ORDER BY year, week_number
    """
    return execute_query(connection, query)


def get_monthly_trends(connection):
    """
    Get monthly aggregated charging statistics.
    """
    query = """
        SELECT 
            DATE_FORMAT(charging_start_time, '%Y-%m') as month,
            YEAR(charging_start_time) as year,
            MONTH(charging_start_time) as month_number,
            DATE_FORMAT(charging_start_time, '%M %Y') as month_name,
            COUNT(*) as total_sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh,
            ROUND(SUM(charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT charging_station_id) as stations_used
        FROM ev_charging_data
        GROUP BY DATE_FORMAT(charging_start_time, '%Y-%m')
        ORDER BY month
    """
    return execute_query(connection, query)


def get_trend_comparison(connection):
    """
    Compare daily, weekly, and monthly trends side by side.
    """
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
                WEEK(charging_start_time) as week,
                COUNT(*) as weekly_sessions,
                SUM(energy_consumed_kwh) as weekly_energy,
                SUM(charging_cost_eur) as weekly_revenue
            FROM ev_charging_data
            GROUP BY WEEK(charging_start_time)
        ) weekly_stats
        
        UNION ALL
        
        SELECT 
            'Monthly Average' as period_type,
            ROUND(AVG(monthly_sessions), 2) as avg_sessions,
            ROUND(AVG(monthly_energy), 2) as avg_energy_kwh,
            ROUND(AVG(monthly_revenue), 2) as avg_revenue
        FROM (
            SELECT 
                MONTH(charging_start_time) as month,
                COUNT(*) as monthly_sessions,
                SUM(energy_consumed_kwh) as monthly_energy,
                SUM(charging_cost_eur) as monthly_revenue
            FROM ev_charging_data
            GROUP BY MONTH(charging_start_time)
        ) monthly_stats
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


def get_energy_by_station(connection, limit=5):
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
    """
    Daily energy delivery trends.
    """
    query = """
        SELECT 
            DATE(charging_start_time) as date,
            COUNT(*) as sessions,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_kwh,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_kwh,
            ROUND(SUM(energy_consumed_kwh) / SUM(charging_duration_hours), 2) as avg_power_kw
        FROM ev_charging_data
        WHERE energy_consumed_kwh IS NOT NULL 
              AND charging_duration_hours > 0
        GROUP BY DATE(charging_start_time)
        ORDER BY date
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
    Distribution of charging sessions by hour of day.
    """
    query = """
        SELECT 
            HOUR(charging_start_time) as hour,
            COUNT(*) as session_count,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration
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
    """
    Daily cost trends over time.
    """
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
        ORDER BY date
    """
    return execute_query(connection, query)


def get_weekly_cost_trends(connection):
    """
    Weekly cost trends over time.
    """
    query = """
        SELECT 
            DATE_FORMAT(charging_start_time, '%Y-W%u') as year_week,
            MIN(DATE(charging_start_time)) as week_start,
            COUNT(*) as sessions,
            ROUND(SUM(charging_cost_eur), 2) as total_cost,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost_per_session
        FROM ev_charging_data
        WHERE charging_cost_eur IS NOT NULL
        GROUP BY DATE_FORMAT(charging_start_time, '%Y-W%u')
        ORDER BY year_week
    """
    return execute_query(connection, query)


def get_monthly_cost_trends(connection):
    """
    Monthly cost trends over time.
    """
    query = """
        SELECT 
            DATE_FORMAT(charging_start_time, '%Y-%m') as month,
            DATE_FORMAT(charging_start_time, '%M %Y') as month_name,
            COUNT(*) as sessions,
            ROUND(SUM(charging_cost_eur), 2) as total_cost,
            ROUND(AVG(charging_cost_eur), 2) as avg_cost_per_session,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy,
            ROUND(SUM(charging_cost_eur) / SUM(energy_consumed_kwh), 2) as cost_per_kwh
        FROM ev_charging_data
        WHERE charging_cost_eur IS NOT NULL 
              AND energy_consumed_kwh > 0
        GROUP BY DATE_FORMAT(charging_start_time, '%Y-%m')
        ORDER BY month
    """
    return execute_query(connection, query)


def get_cost_trends_by_time_of_day(connection):
    """
    Cost trends by time of day over time periods.
    """
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
        ORDER BY date, time_of_day
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


def get_top_users_ranking(connection, limit=5):
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
    Comprehensive usage statistics per charging station.
    """
    query = """
        SELECT 
            charging_station_id,
            COUNT(*) as total_sessions,
            COUNT(DISTINCT user_id) as unique_users,
            ROUND(SUM(charging_cost_eur), 2) as total_revenue,
            ROUND(AVG(charging_cost_eur), 2) as avg_revenue_per_session,
            ROUND(SUM(energy_consumed_kwh), 2) as total_energy_delivered,
            ROUND(AVG(energy_consumed_kwh), 2) as avg_energy_per_session,
            ROUND(AVG(charging_duration_hours), 2) as avg_duration_hours,
            ROUND(AVG(charging_rate_kw), 2) as avg_charging_rate_kw,
            COUNT(DISTINCT DATE(charging_start_time)) as days_active
        FROM ev_charging_data
        GROUP BY charging_station_id
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


def get_top_stations_ranking(connection, limit=5):
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