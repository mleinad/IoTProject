-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS IoT_project;

-- Create a dedicated user for container connections
CREATE USER IF NOT EXISTS 'iot_user'@'%' IDENTIFIED BY 'iot_password';

-- Grant full privileges on the database
GRANT ALL PRIVILEGES ON IoT_project.* TO 'iot_user'@'%';

FLUSH PRIVILEGES;
