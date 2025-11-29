-- Create Real-time Charging Sessions table (for MQTT data)
CREATE TABLE IF NOT EXISTS ev_charging_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_model VARCHAR(100),
    battery_capacity_kwh DECIMAL(5,2),
    charging_station_id VARCHAR(50),
    energy_consumed_kwh DECIMAL(6,2),
    charging_duration_hours DECIMAL(5,3),
    charging_rate_kw DECIMAL(6,2),
    charging_cost_eur DECIMAL(8,2),
    time_of_day VARCHAR(20),
    day_of_week VARCHAR(20),
    state_of_charge_start_pct DECIMAL(5,2),
    state_of_charge_end_pct DECIMAL(5,2),
    distance_driven_km DECIMAL(7,2),
    temperature_c DECIMAL(4,2),
    vehicle_age_years INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_station_id (charging_station_id),
    INDEX idx_vehicle_model (vehicle_model),
    INDEX idx_day_week (day_of_week),
    INDEX idx_time_of_day (time_of_day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;