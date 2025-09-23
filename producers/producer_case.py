"""
producer_meyer.py

Manufacturing sensor data streaming producer.
Generates realistic machine sensor data for temperature, vibration monitoring,
and efficiency analysis.

Example JSON message:
{
    "message": "Machine 15 in Active mode. Temp: 44.28°C, Vib: 2.08Hz.",
    "author": "Sensor-15",
    "timestamp": "2024-01-01 00:02:00",
    "category": "active",
    "sentiment": 0.99,
    "keyword_mentioned": "Low",
    "message_length": 54
}
"""

#####################################
# Import Modules
#####################################

# stdlib
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime
from typing import Mapping, Any

# external
from kafka import KafkaProducer  # kafka-python-ng

# local
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

# optional local emitters (single-responsibility helpers)
from utils.emitters import file_emitter, kafka_emitter, sqlite_emitter, duckdb_emitter


#####################################
# Manufacturing Data Generator
#####################################

def generate_manufacturing_messages():
    """Yield manufacturing sensor JSON messages forever."""
    
    # Manufacturing equipment configurations
    MACHINE_IDS = range(1, 51)  # 50 machines
    MODES = ["Active", "Idle", "Maintenance", "Offline"]
    MODE_WEIGHTS = [0.6, 0.25, 0.1, 0.05]  # Active is most common
    
    # Temperature ranges by mode (Celsius)
    TEMP_RANGES = {
        "Active": (35, 95),      # Operating temperature
        "Idle": (70, 90),        # Warm but not working
        "Maintenance": (20, 30), # Room temperature
        "Offline": (18, 25)      # Cool down
    }
    
    # Vibration ranges by mode (Hz)
    VIB_RANGES = {
        "Active": (0.1, 4.0),    # Normal operation vibration
        "Idle": (0.0, 1.0),      # Minimal vibration
        "Maintenance": (0.0, 0.5), # Very low
        "Offline": (0.0, 0.1)    # Almost none
    }
    
    # Efficiency categories based on temperature and vibration
    EFFICIENCY_KEYWORDS = ["Low", "Medium", "High", "Critical"]
    
    message_count = 0
    base_time = datetime(2024, 1, 1, 0, 0, 0)
    
    while True:
        machine_id = random.choice(MACHINE_IDS)
        mode = random.choices(MODES, weights=MODE_WEIGHTS)[0]
        
        # Generate realistic sensor readings based on mode
        temp_min, temp_max = TEMP_RANGES[mode]
        vib_min, vib_max = VIB_RANGES[mode]
        
        temperature = round(random.uniform(temp_min, temp_max), 2)
        vibration = round(random.uniform(vib_min, vib_max), 2)
        
        # Calculate efficiency keyword based on readings
        if mode == "Offline":
            efficiency = "Critical"
            sentiment = 0.1
        elif mode == "Maintenance":
            efficiency = "Low" 
            sentiment = 0.3
        elif mode == "Idle":
            efficiency = random.choice(["Low", "Medium"])
            sentiment = random.uniform(0.4, 0.7)
        else:  # Active mode
            # High temp or high vibration = lower efficiency
            if temperature > 80 or vibration > 3.0:
                efficiency = "Medium"
                sentiment = random.uniform(0.5, 0.8)
            elif temperature > 70 or vibration > 2.0:
                efficiency = "Medium"
                sentiment = random.uniform(0.6, 0.9)
            else:
                efficiency = "High"
                sentiment = random.uniform(0.8, 1.0)
        
        # Create message text
        message_text = f"Machine {machine_id} in {mode} mode. Temp: {temperature}°C, Vib: {vibration:.2f}Hz."
        
        # Calculate timestamp (increment by 1 minute each message)
        timestamp = base_time.replace(minute=message_count % 60, 
                                     hour=(message_count // 60) % 24)
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        yield {
            "message": message_text,
            "author": f"Sensor-{machine_id}",
            "timestamp": timestamp_str,
            "category": mode.lower(),
            "sentiment": round(sentiment, 2),
            "keyword_mentioned": efficiency,
            "message_length": len(message_text),
            "machine_id": machine_id,
            "temperature": temperature,
            "vibration": vibration,
            "efficiency_level": efficiency
        }
        
        message_count += 1


#####################################
# Single-responsibility emitters (per-sink)
#####################################

def emit_to_file(message: Mapping[str, Any], *, path: pathlib.Path) -> bool:
    """Append one message to a JSONL file."""
    return file_emitter.emit_message(message, path=path)


def emit_to_kafka(
    message: Mapping[str, Any], *, producer: KafkaProducer, topic: str
) -> bool:
    """Publish one message to a Kafka topic."""
    return kafka_emitter.emit_message(message, producer=producer, topic=topic)


def emit_to_sqlite(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """Insert one message into SQLite."""
    return sqlite_emitter.emit_message(message, db_path=db_path)


def emit_to_duckdb(message: Mapping[str, Any], *, db_path: pathlib.Path) -> bool:
    """Insert one message into DuckDB."""
    return duckdb_emitter.emit_message(message, db_path=db_path)


#####################################
# Main
#####################################

def main() -> None:
    logger.info("Starting Manufacturing Sensor Producer")
    logger.info("Streaming machine sensor data with temperature and vibration readings")
    logger.info("Use Ctrl+C to stop.")

    # STEP 1. Read config
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
        # Optional DB paths (fallbacks if not provided)
        sqlite_path: pathlib.Path = (
            config.get_sqlite_path() if hasattr(config, "get_sqlite_path")
            else pathlib.Path("data/manufacturing.sqlite")
        )
        duckdb_path: pathlib.Path = (
            config.get_duckdb_path() if hasattr(config, "get_duckdb_path")
            else pathlib.Path("data/manufacturing.duckdb")
        )
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # STEP 2. Reset file sink (fresh run)
    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to prep live data file: {e}")
        sys.exit(2)

    # STEP 3. (Optional) Setup Kafka
    producer = None
    try:
        # Soft-check: do not exit if Kafka is down
        if verify_services(strict=False):
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            logger.info(f"Kafka producer connected to {kafka_server}")
            try:
                create_kafka_topic(topic)
                logger.info(f"Kafka topic '{topic}' is ready.")
            except Exception as e:
                logger.warning(f"WARNING: Topic create/verify failed ('{topic}'): {e}")
        else:
            logger.info("Kafka disabled for this run.")
    except Exception as e:
        logger.warning(f"WARNING: Kafka setup failed: {e}")
        producer = None

    # STEP 4. Emit loop — Manufacturing sensor data
    try:
        for message in generate_manufacturing_messages():
            logger.info(f"Machine {message['machine_id']}: {message['category']} - "
                       f"Temp: {message['temperature']}°C, Vib: {message['vibration']}Hz, "
                       f"Efficiency: {message['efficiency_level']}")

            # --- File (JSONL) ---
            emit_to_file(message, path=live_data_path)

            # --- Kafka ---
            if producer is not None:
                emit_to_kafka(message, producer=producer, topic=topic)

            # --- SQLite ---
            # Uncomment to enable SQLite sink:
            # emit_to_sqlite(message, db_path=sqlite_path)

            # --- DuckDB ---
            # Uncomment to enable DuckDB sink:
            # emit_to_duckdb(message, db_path=duckdb_path)

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Manufacturing Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            try:
                producer.flush(timeout=5)
                producer.close()
                logger.info("Kafka producer closed.")
            except Exception:
                pass
        logger.info("Manufacturing Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()