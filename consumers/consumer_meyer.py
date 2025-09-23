"""
consumer_meyer.py

Manufacturing Analytics Consumer with Real-time Visualization

This consumer processes manufacturing sensor data streams and provides:
1. Real-time equipment efficiency monitoring
2. Temperature and vibration trend analysis
3. Dynamic visualization of machine performance
4. Anomaly detection and alerting

Key Insights:
- Equipment efficiency based on temperature and vibration thresholds
- Machine utilization patterns across different modes
- Real-time anomaly detection for predictive maintenance
"""

import json
import sqlite3
import pathlib
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque, defaultdict
from datetime import datetime
from typing import Optional, Dict, Any, List
import re

# Configuration
DATA_FILE = pathlib.Path('data/project_live.json')
DB_FILE = pathlib.Path('data/meyer_manufacturing_analytics.sqlite')

# Real-time data storage
temperature_data = defaultdict(lambda: deque(maxlen=20))  # Last 20 readings per machine
vibration_data = defaultdict(lambda: deque(maxlen=20))
efficiency_scores = defaultdict(lambda: deque(maxlen=20))
timestamps = deque(maxlen=20)
machine_status = {}  # Current status of each machine

# Analytics tracking
processed_messages = set()
efficiency_stats = defaultdict(list)
anomaly_count = 0

def init_db(db_file: pathlib.Path) -> None:
    """Initialize SQLite database for manufacturing analytics."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS manufacturing_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id INTEGER,
            timestamp TEXT,
            mode TEXT,
            temperature REAL,
            vibration REAL,
            efficiency_score REAL,
            efficiency_level TEXT,
            is_anomaly BOOLEAN,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Manufacturing analytics database initialized at {db_file}")

def calculate_efficiency_score(temperature: float, vibration: float, mode: str) -> tuple[float, bool]:
    """
    Calculate equipment efficiency score based on sensor readings.
    
    Returns: (efficiency_score, is_anomaly)
    """
    base_score = 100
    is_anomaly = False
    
    # Mode-based scoring
    if mode == "offline":
        return 0.0, True
    elif mode == "maintenance":
        return 20.0, False
    elif mode == "idle":
        base_score = 60
    
    # Temperature penalties/bonuses
    if 40 <= temperature <= 75:  # Optimal range
        temp_modifier = 1.0
    elif temperature > 90:  # Too hot - major penalty
        temp_modifier = 0.5
        is_anomaly = True
    elif temperature > 80:  # Hot - penalty
        temp_modifier = 0.7
    elif temperature < 30:  # Too cold for active operation
        temp_modifier = 0.8
        is_anomaly = True
    else:
        temp_modifier = 0.9
    
    # Vibration penalties
    if vibration <= 1.5:  # Low vibration - good
        vib_modifier = 1.0
    elif vibration <= 2.5:  # Medium vibration
        vib_modifier = 0.9
    elif vibration <= 3.5:  # High vibration - concerning
        vib_modifier = 0.7
    else:  # Very high vibration - critical
        vib_modifier = 0.4
        is_anomaly = True
    
    efficiency_score = min(100, base_score * temp_modifier * vib_modifier)
    return round(efficiency_score, 1), is_anomaly

def extract_sensor_data(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract temperature and vibration from message text using regex."""
    message_text = message.get("message", "")
    
    # Extract temperature: "Temp: 44.28°C"
    temp_match = re.search(r"Temp: ([\d.]+)°C", message_text)
    # Extract vibration: "Vib: 2.08Hz"
    vib_match = re.search(r"Vib: ([\d.]+)Hz", message_text)
    # Extract machine ID: "Machine 15"
    machine_match = re.search(r"Machine (\d+)", message_text)
    
    if not all([temp_match, vib_match, machine_match]):
        return None
        
    return {
        "machine_id": int(machine_match.group(1)),
        "temperature": float(temp_match.group(1)),
        "vibration": float(vib_match.group(1)),
        "mode": message.get("category", "unknown"),
        "timestamp": message.get("timestamp"),
        "author": message.get("author")
    }

def process_and_store_message(message: Dict[str, Any], db_file: pathlib.Path) -> None:
    """Process manufacturing message and store analytics."""
    global anomaly_count
    
    sensor_data = extract_sensor_data(message)
    if not sensor_data:
        print("⚠️ Could not extract sensor data from message")
        return
    
    machine_id = sensor_data["machine_id"]
    temperature = sensor_data["temperature"]
    vibration = sensor_data["vibration"]
    mode = sensor_data["mode"]
    
    # Calculate efficiency metrics
    efficiency_score, is_anomaly = calculate_efficiency_score(temperature, vibration, mode)
    
    # Update real-time data for visualization
    temperature_data[machine_id].append(temperature)
    vibration_data[machine_id].append(vibration)
    efficiency_scores[machine_id].append(efficiency_score)
    
    # Update machine status
    machine_status[machine_id] = {
        "mode": mode,
        "temperature": temperature,
        "vibration": vibration,
        "efficiency": efficiency_score,
        "is_anomaly": is_anomaly
    }
    
    # Track analytics
    efficiency_stats[mode].append(efficiency_score)
    if is_anomaly:
        anomaly_count += 1
        print(f"🚨 ANOMALY DETECTED: Machine {machine_id} - Temp: {temperature}°C, Vib: {vibration}Hz")
    
    # Store in database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO manufacturing_analytics 
        (machine_id, timestamp, mode, temperature, vibration, efficiency_score, efficiency_level, is_anomaly)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        machine_id,
        sensor_data["timestamp"],
        mode,
        temperature,
        vibration,
        efficiency_score,
        message.get("keyword_mentioned", "Unknown"),
        is_anomaly
    ))
    
    conn.commit()
    conn.close()
    
    print(f"📊 Machine {machine_id}: {mode.upper()} | "
          f"Efficiency: {efficiency_score}% | "
          f"Temp: {temperature}°C | Vib: {vibration}Hz")

def read_latest_messages(data_file: pathlib.Path) -> List[Dict[str, Any]]:
    """Read all new messages from the live data file."""
    global processed_messages
    
    if not data_file.exists():
        return []
    
    new_messages = []
    try:
        with open(data_file, 'r') as f:
            lines = f.readlines()
            
        for line in lines:
            if line.strip():
                message = json.loads(line.strip())
                msg_id = f"{message.get('author')}_{message.get('timestamp')}"
                
                if msg_id not in processed_messages:
                    new_messages.append(message)
                    processed_messages.add(msg_id)
                    
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error reading messages: {e}")
    
    return new_messages

def setup_dynamic_plot():
    """Setup matplotlib for dynamic visualization."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Real-time Manufacturing Analytics Dashboard', fontsize=16, fontweight='bold')
    
    return fig, (ax1, ax2, ax3, ax4)

def animate_dashboard(frame, fig, axes):
    """Animation function for real-time dashboard updates."""
    ax1, ax2, ax3, ax4 = axes
    
    # Clear all axes
    for ax in axes:
        ax.clear()
    
    # Chart 1: Real-time Efficiency Trends
    ax1.set_title('Real-time Equipment Efficiency', fontweight='bold')
    ax1.set_ylabel('Efficiency Score (%)')
    ax1.set_xlabel('Time (Recent Readings)')
    ax1.set_ylim(0, 100)
    
    for machine_id, scores in efficiency_scores.items():
        if scores:
            ax1.plot(range(len(scores)), scores, 'o-', 
                    label=f'Machine {machine_id}', linewidth=2, markersize=4)
    
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # Chart 2: Current Machine Status
    ax2.set_title('Current Machine Status Distribution', fontweight='bold')
    if machine_status:
        modes = [status['mode'] for status in machine_status.values()]
        mode_counts = {}
        for mode in modes:
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
        
        colors = {'active': 'green', 'idle': 'yellow', 'maintenance': 'orange', 'offline': 'red'}
        bar_colors = [colors.get(mode, 'gray') for mode in mode_counts.keys()]
        
        ax2.bar(mode_counts.keys(), mode_counts.values(), color=bar_colors)
        ax2.set_ylabel('Number of Machines')
        
        # Add value labels on bars
        for mode, count in mode_counts.items():
            ax2.text(mode, count + 0.1, str(count), ha='center', fontweight='bold')
    
    # Chart 3: Temperature vs Vibration Scatter
    ax3.set_title('Temperature vs Vibration Analysis', fontweight='bold')
    ax3.set_xlabel('Temperature (°C)')
    ax3.set_ylabel('Vibration (Hz)')
    
    if machine_status:
        temps = []
        vibs = []
        colors = []
        for status in machine_status.values():
            temps.append(status['temperature'])
            vibs.append(status['vibration'])
            if status['is_anomaly']:
                colors.append('red')
            elif status['mode'] == 'active':
                colors.append('green')
            elif status['mode'] == 'idle':
                colors.append('yellow')
            else:
                colors.append('gray')
        
        scatter = ax3.scatter(temps, vibs, c=colors, alpha=0.7, s=60)
        ax3.grid(True, alpha=0.3)
        
        # Add threshold lines
        ax3.axhline(y=3.5, color='red', linestyle='--', alpha=0.5, label='High Vibration Threshold')
        ax3.axvline(x=90, color='red', linestyle='--', alpha=0.5, label='High Temperature Threshold')
        ax3.legend()
    
    # Chart 4: Efficiency Summary Stats
    ax4.set_title('Efficiency Statistics by Mode', fontweight='bold')
    if efficiency_stats:
        modes = list(efficiency_stats.keys())
        avg_efficiency = [sum(scores)/len(scores) if scores else 0 for scores in efficiency_stats.values()]
        
        bars = ax4.bar(modes, avg_efficiency, 
                      color=['green' if avg >= 80 else 'yellow' if avg >= 60 else 'red' 
                             for avg in avg_efficiency])
        ax4.set_ylabel('Average Efficiency (%)')
        ax4.set_ylim(0, 100)
        
        # Add value labels
        for bar, avg in zip(bars, avg_efficiency):
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                    f'{avg:.1f}%', ha='center', fontweight='bold')
    
    # Add anomaly counter
    fig.text(0.02, 0.02, f'Anomalies Detected: {anomaly_count}', 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="red", alpha=0.3),
             fontsize=12, fontweight='bold')
    
    plt.tight_layout()

def main():
    """Main consumer with real-time visualization."""
    print("=== Manufacturing Analytics Consumer ===")
    print("Real-time processing of manufacturing sensor data")
    print(f"Reading from: {DATA_FILE}")
    print(f"Storing analytics in: {DB_FILE}")
    print("Starting dynamic visualization...")
    
    # Initialize database
    init_db(DB_FILE)
    
    # Setup matplotlib for real-time plotting
    plt.ion()  # Interactive mode
    fig, axes = setup_dynamic_plot()
    
    # Create animation
    ani = animation.FuncAnimation(fig, animate_dashboard, fargs=(fig, axes), 
                                 interval=2000, cache_frame_data=False)
    
    try:
        # Start the processing loop
        while True:
            # Process new messages
            messages = read_latest_messages(DATA_FILE)
            
            for message in messages:
                process_and_store_message(message, DB_FILE)
            
            if not messages:
                print("⏳ Waiting for new sensor data...")
            
            # Update the plot
            plt.pause(0.1)
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n🛑 Manufacturing Analytics Consumer stopped by user")
    except Exception as e:
        print(f"❌ Error in consumer: {e}")
    finally:
        plt.ioff()
        print("Consumer shutting down...")
        print(f"📈 Total anomalies detected: {anomaly_count}")
        print(f"📊 Processed data for {len(machine_status)} machines")

if __name__ == "__main__":
    main()