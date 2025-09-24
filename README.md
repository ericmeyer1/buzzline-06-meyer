# Manufacturing Analytics Streaming Pipeline

## Project Overview

This custom streaming pipeline processes real-time manufacturing sensor data to provide equipment efficiency monitoring, anomaly detection, and predictive maintenance insights. The system analyzes temperature and vibration readings from factory equipment to optimize production and prevent costly downtime.

## Key Features

- **Real-time Equipment Monitoring**: Tracks manufacturing machines with temperature and vibration sensors
- **Efficiency Scoring**: Calculates equipment efficiency based on optimal operating ranges
- **Anomaly Detection**: Identifies critical conditions requiring immediate attention
- **Dynamic Visualization**: Dashboard with live updates
- **Predictive Analytics**: Stores historical data for trend analysis and maintenance planning

## How It Works

### Producer (`producer_case.py`)
Generates realistic manufacturing sensor data simulating:
- Machines in various operational modes
- Temperature readings based on equipment state
- Vibration measurements indicating wear and performance
- Efficiency classifications (Low, Medium, High, Critical)

### Consumer (`consumer_meyer.py`)
Processes each message in real-time to:
1. **Extract sensor readings** from message text
2. **Calculate efficiency scores** based on temperature/vibration thresholds
3. **Detect anomalies** when readings exceed safe operating limits
4. **Store analytics** in SQLite database for historical analysis
5. **Update visualization** with current machine status and trends

This approach provides immediate operational visibility while building historical datasets for predictive maintenance strategies.

## Dynamic Visualization

The consumer displays a comprehensive and real time dashboard:

1. **Equipment Efficiency Trends**
   - Line chart showing real-time efficiency scores per machine
   - X-axis: Time , Y-axis: Efficiency percentage
   - Multiple colored lines for different machines

2. **Machine Status Distribution**
   - Bar chart of current operational modes
   - Color-coded: Active (green), Idle (yellow), Maintenance (orange), Offline (red)
   - Shows live equipment utilization

3. **Temperature vs Vibration Analysis**
   - Scatter plot correlating sensor readings
   - Red dots indicate anomalous conditions
   - Threshold lines mark safe operating limits

4. **Efficiency Statistics by Mode**
   - Bar chart of average efficiency by operational state
   - Color-coded performance levels

## Running the Pipeline

### Prerequisites
```bash
# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies  
pip install -r requirements.txt
```

### Start the Producer
```bash
# Generate manufacturing sensor data
python3.11 -m producers.producer_case.py
```

### Start the Consumer (separate terminal)
```bash
# Process data with real-time visualization
python3.11 -m consumers.consumer_meyer.py
```

The consumer will open a matplotlib window displaying the live dashboard that updates every 2 seconds as new sensor data arrives.

## Data Storage

**Database**: SQLite (`data/meyer_manufacturing_analytics.sqlite`)

**Schema**:
```sql
CREATE TABLE manufacturing_analytics (
    id INTEGER PRIMARY KEY,
    machine_id INTEGER,
    timestamp TEXT,
    mode TEXT,
    temperature REAL,
    vibration REAL,
    efficiency_score REAL,
    efficiency_level TEXT,
    is_anomaly BOOLEAN,
    processed_at TIMESTAMP
);
```

## Technical Implementation

**Data Source**: Local file streaming (project_live.json)  
**Processing**: One message at a time with real-time analytics  
**Visualization**: Matplotlib with animation for dynamic updates  
**Storage**: SQLite for persistence and historical analysis  
**Anomaly Logic**: Threshold-based detection with manufacturing-specific rules