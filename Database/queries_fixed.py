from mysql.connector import Error

def execute_query(connection, query, params=None):
    """Execute a SELECT query and return results as a list of tuples."""
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Error as e:
        print(f"Error executing query: {e}")
        return None


# ========================================
# BASIC STATS (These work fine)
# ========================================

def get_total_energy_delivered(connection):
    """Get overall energy delivery statistics."""
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


def get_cost_statistics(connection):
    """Overall cost statistics across all charging sessions."""
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
# TIME DISTRIBUTIONS (These work fine)
# ========================================

def get_time_of_day_distribution(connection):
    """Distribution of charging sessions by time of day."""
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


def get_day_of_week_distribution(connection):
    """Distribution of charging sessions by day of week."""
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


# ========================================
# TRENDS - PROPERLY FIXED
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
            YEARWEEK(charging_start_time, 1) as yearweek,
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
        GROUP BY YEARWEEK(charging_start_time, 1)
        ORDER BY YEARWEEK(charging_start_time, 1)
    """
    return execute_query(connection, query)


def get_monthly_trends(connection):
    """Get monthly aggregated charging statistics."""
    query = """
        SELECT 
            DATE_FORMAT(charging_start_time, '%Y-%m') as month,
            MIN(DATE(charging_start_time)) as month_start,
            MAX(DATE(charging_start_time)) as month_end,
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
        ORDER BY DATE_FORMAT(charging_start_time, '%Y-%m')
    """
    return execute_query(connection, query)


# ========================================
# USER ANALYTICS
# ========================================

def get_usage_per_user(connection):
    """Comprehensive usage statistics per user."""
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


# ========================================
# STATION ANALYTICS
# ========================================

def get_power_distribution(connection):
    """Analyze distribution of station power capacities."""
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
        ORDER BY avg_power
    """
    return execute_query(connection, query)


def get_stations_by_distrito(connection):
    """Get station count and statistics by distrito."""
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
