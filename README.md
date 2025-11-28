# EV Charging Infrastructure Management - IoT Project

**Course:** Internet das Coisas (IoT) - MEI/MI/MSI/MCD 2025/26  
**Institution:** Departamento de Informática, Faculdade de Ciências da Universidade de Lisboa  
**Project Stage:** Stage 1 - Real-time Monitoring System

---

## 📋 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Services](#services)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Stage 2 Roadmap](#stage-2-roadmap)
- [References](#references)

---

## 🎯 Overview

This project implements an IoT data platform for monitoring and analyzing Electric Vehicle (EV) charging networks. The system collects, processes, and visualizes real-time data from charging stations, enabling operators to identify patterns, optimize infrastructure, and improve user experience.

### Key Objectives
- Real-time monitoring of EV charging sessions
- Statistical analysis of charging patterns
- Interactive dashboard for data exploration
- Scalable microservices architecture using Docker
- Foundation for machine learning integration (Stage 2)

---

## 🏗️ System Architecture

┌─────────────────────────────────────────────────────────────────┐
│                    CLOUD PLATFORM (Docker)                      │
│                                                                 │
│  ┌──────────────┐         ┌──────────────┐                      │
│  │ MQTT Broker  │◄────────│ MQTT         │                      │
│  │ (Mosquitto)  │  Topic: │ Publisher    │ (Charging Stations)  │
│  │ Port: 1883   │ idc/fc01│ (Simulator)  │                      │
│  └──────┬───────┘         └──────────────┘                      │
│         │                                                       │
│         ↓                                                       │
│  ┌──────────────┐         ┌──────────────┐                      │
│  │ MQTT         │────────→│ Processor    │                      │
│  │ Subscriber   │         │ Service      │                      │
│  └──────────────┘         └──────┬───────┘                      │
│                                   │                             │
│                                   ↓                             │
│                         ┌──────────────┐                        │
│                         │   MySQL DB   │                        │
│                         │  Port: 3306  │                        │
│                         └──────────────┘                        │
│                                                                 │
│                         ┌──────────────┐                        │
│                         │  Dashboard   │◄───── Web Browser      │
│                         │ (Streamlit)  │       (Users)          │
│                         │  Port: 8501  │                        │
│                         └──────────────┘                        │
└─────────────────────────────────────────────────────────────────┘

---

## ✨ Features

### Stage 1 (Current Implementation)

- **Real-time Data Ingestion**
  - MQTT-based communication protocol
  - Simulated charging station data publishing
  - Message queuing and processing

- **Data Processing**
  - Aggregated statistics (sessions, energy, cost)
  - User and station tracking
  - Time-series data handling

- **Interactive Dashboard**
  - Live metrics display
  - Recent charging sessions table
  - Energy consumption trends
  - Cost distribution analysis
  - Auto-refresh capability

- **Containerized Deployment**
  - Docker Compose orchestration
  - Isolated microservices
  - Easy scalability

---

## 🔧 Prerequisites

### Required Software
- **Docker** (version 20.10 or higher)
- **Docker Compose** (version 2.0 or higher)
- **Python** 3.11+ (for running publisher outside Docker)

### Optional
- Git (for version control)
- Make (for simplified commands)

### System Requirements
- **RAM:** Minimum 4GB (8GB recommended)
- **Disk Space:** 5GB free space
- **OS:** Linux, macOS, or Windows with WSL2

---

## 📥 Installation

### 1. Clone the Repository

git clone <repository-url>
cd IoTProject

### 2. Project Structure Setup

Ensure your project has the following structure:

IoTProject/
├── Communication/
│   ├── config/
│   │   └── mosquitto.conf
│   ├── mqtt_processor.py
│   └── mqtt_publisher.py
├── Dashboard/
│   └── streamlit_dashboard.py
├── Data/
│   ├── EV-Stations_with_ids_coords.csv
│   └── EV_with_stations.csv
├── Database/
│   ├── data_sql/
│   │   ├── create_tables.py
│   │   ├── extract_data.py
│   │   ├── mysql_connector.py
│   │   └── queries.py
│   └── main.py
├── docker-compose.yml
├── Dockerfile.processor
├── Dockerfile.dashboard
├── .env
└── README.md

### 3. Configure Environment Variables

Create a `.env` file in the project root:

#### MySQL Configuration
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_DB_USER=iot_user
MYSQL_DB_PASSWORD=iot_password

#### MQTT Configuration
MQTT_TOPIC=idc/fc01

### 4. Create MQTT Configuration

Create `Communication/config/mosquitto.conf`:

listener 1883
allow_anonymous true
persistence true
persistence_location /mosquitto/data/
log_dest file /mosquitto/log/mosquitto.log

---

## 🚀 Usage

### Starting the System

#### Option 1: Quick Start (All Services)

##### Start all services in detached mode
docker-compose up --build -d

##### Verify all services are running
docker-compose ps

Expected output:
NAME                 STATUS
iot-mqtt-broker      Up
iot-mysql-db         Up (healthy)
iot-mqtt-processor   Up
iot-dashboard        Up

#### Option 2: Development Mode (with logs)

##### Start with live logs
docker-compose up --build

### Running the Publisher (Charging Station Simulator)

In a new terminal:

#### Activate virtual environment (if using one)
source venv/bin/activate  # Linux/Mac
#### or
venv\Scripts\activate  # Windows

#### Run the publisher
cd Communication
python3 mqtt_publisher.py

The publisher will:
- Load the dataset from `Data/EV_with_stations.csv`
- Publish 50 charging sessions at 2-second intervals
- Display progress in the terminal

### Accessing the Dashboard

Open your web browser and navigate to:

http://localhost:8501

**Dashboard Features:**
- Click **🔄 Refresh** to update with latest data
- Enable **Auto-refresh (5s)** in sidebar for automatic updates
- View real-time metrics, charts, and session tables

---

## 📊 Monitoring and Logs

### View Service Logs

#### All services
docker-compose logs -f

#### Specific service
docker-compose logs -f dashboard
docker-compose logs -f mqtt-processor
docker-compose logs -f mqtt-broker
docker-compose logs -f mysql-db

### Check Service Status

#### List running containers
docker-compose ps

#### Check resource usage
docker stats

### Restart Services

#### Restart all services
docker-compose restart

#### Restart specific service
docker-compose restart dashboard

---

## 📁 Project Structure

IoTProject/
│
├── Communication/              # MQTT communication layer
│   ├── config/
│   │   └── mosquitto.conf     # MQTT broker configuration
│   ├── mqtt_processor.py      # MQTT subscriber & data processor
│   └── mqtt_publisher.py      # Charging station simulator
│
├── Dashboard/                  # Visualization layer
│   └── streamlit_dashboard.py # Interactive web dashboard
│
├── Data/                       # Datasets
│   ├── EV-Stations_with_ids_coords.csv  # Station metadata
│   └── EV_with_stations.csv             # Charging sessions
│
├── Database/                   # Database services
│   ├── data_sql/              # SQL scripts and utilities
│   └── main.py                # Database initialization
│
├── docker-compose.yml         # Service orchestration
├── Dockerfile.processor       # Processor service image
├── Dockerfile.dashboard       # Dashboard service image
├── .env                       # Environment variables
└── README.md                  # This file

---

## 🔌 Services

### 1. MQTT Broker (Mosquitto)
- **Container:** `eclipse-mosquitto:latest`
- **Ports:** 1883 (MQTT), 9001 (WebSockets)
- **Purpose:** Message broker for pub/sub communication
- **Configuration:** `Communication/config/mosquitto.conf`

### 2. MQTT Publisher (Simulator)
- **Technology:** Python 3.11
- **Libraries:** `paho-mqtt`, `pandas`
- **Purpose:** Simulates charging stations sending real-time data
- **Data Source:** `Data/EV_with_stations.csv`
- **Message Rate:** 1 message every 2 seconds (configurable)

### 3. MQTT Processor
- **Container:** Custom Docker image
- **Technology:** Python 3.11
- **Libraries:** `paho-mqtt`, `mysql-connector-python`, `pandas`
- **Purpose:** Subscribe to MQTT, process data, update statistics
- **Features:**
  - Real-time message processing
  - Statistical aggregation
  - User and station tracking

### 4. MySQL Database
- **Container:** `mysql:8.0`
- **Port:** 3306
- **Database:** `IoT_project`
- **Credentials:** See `.env` file
- **Purpose:** Persistent storage for charging data

### 5. Dashboard (Streamlit)
- **Container:** Custom Docker image
- **Technology:** Python 3.11 + Streamlit
- **Libraries:** `streamlit`, `paho-mqtt`, `pandas`, `plotly`
- **Port:** 8501
- **Purpose:** Interactive web-based visualization

---

## ⚙️ Configuration

### MQTT Configuration

Edit `Communication/config/mosquitto.conf`:

#### Listener port
listener 1883

#### Allow anonymous connections (for development)
allow_anonymous true

#### Persistence
persistence true
persistence_location /mosquitto/data/

#### Logging
log_dest file /mosquitto/log/mosquitto.log
log_type all

### Database Configuration

Edit `.env`:

MYSQL_ROOT_PASSWORD=your_root_password
MYSQL_DB_USER=your_username
MYSQL_DB_PASSWORD=your_password

### Publisher Configuration

Edit `Communication/mqtt_publisher.py`:

#### Configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPIC = "idc/fc01"
INTERVAL_SECONDS = 2  # Time between messages
NUM_SESSIONS = 50     # Number of sessions to publish

---

## 🐛 Troubleshooting

### Common Issues

#### 1. Port Already in Use

**Error:** `Bind for 0.0.0.0:1883 failed: port is already allocated`

**Solution:**
##### Stop conflicting services
docker-compose down

##### Check what's using the port
sudo lsof -i :1883
sudo lsof -i :8501
sudo lsof -i :3306

##### Kill the process or change ports in docker-compose.yml

#### 2. Dashboard Shows No Data

**Symptoms:** Dashboard displays all zeros, "Waiting for data"

**Solution:**
##### 1. Check if publisher is running
##### 2. Verify processor is receiving messages
docker-compose logs mqtt-processor | grep "Received"

##### 3. Check MQTT broker logs
docker-compose logs mqtt-broker

##### 4. Restart services
docker-compose restart dashboard

##### 5. Click Refresh button in dashboard

#### 3. MySQL Connection Failed

**Error:** `Can't connect to MySQL server`

**Solution:**
##### Wait for MySQL to be healthy
docker-compose up -d
docker-compose logs mysql-db

##### Check health status
docker-compose ps

##### Restart MySQL
docker-compose restart mysql-db

#### 4. Container Build Fails

**Solution:**
##### Clean Docker cache
docker-compose down -v
docker system prune -a

##### Rebuild from scratch
docker-compose build --no-cache
docker-compose up -d

### Checking Service Health

##### Check if all containers are running
docker-compose ps

##### Check logs for errors
docker-compose logs | grep -i error

##### Test MQTT connection
mosquitto_sub -h localhost -p 1883 -t "idc/fc01"

##### Test database connection
docker exec -it iot-mysql-db mysql -u iot_user -p


