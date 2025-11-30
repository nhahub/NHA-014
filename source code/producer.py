import json
import random
import time
import logging
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -----------------------------
# Azure Event Hub Config
# -----------------------------
CONNECTION_STR = ""  
EVENT_HUB_NAME = "receive"  

# -----------------------------
# EventHub Producer
# -----------------------------
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
)

# -----------------------------
# Locations with realistic initial values (based on TomTom/IQAir 2024 data for Egypt)
# -----------------------------
locations = [
    {"sensor_id": "T-001", "location_id": 1, "city": "Cairo", "congestion": random.randint(50, 70), "lat": 30.0444, "lon": 31.2357, "road_length_km": 5},
    {"sensor_id": "T-002", "location_id": 2, "city": "Giza", "congestion": random.randint(45, 65), "lat": 30.0131, "lon": 31.2089, "road_length_km": 4},
    {"sensor_id": "T-003", "location_id": 3, "city": "Alex", "congestion": random.randint(40, 60), "lat": 31.2001, "lon": 29.9187, "road_length_km": 6},
    {"sensor_id": "T-004", "location_id": 4, "city": "Mansoura", "congestion": random.randint(35, 55), "lat": 31.0409, "lon": 31.3785, "road_length_km": 3},
    {"sensor_id": "T-005", "location_id": 5, "city": "Aswan", "congestion": random.randint(30, 50), "lat": 24.0889, "lon": 32.8998, "road_length_km": 2.5}
]

# ---------------------------------------------------
# Helper functions (improved for realism)
# ---------------------------------------------------
def smooth_change(value, step=3):  # Smaller step for smoother changes
    change = random.uniform(-step, step)  # Use uniform for more natural variation
    return max(0, min(100, value + change))

def estimate_emission(congestion, vehicle_count, road_length_km):
    base_per_vehicle = 0.18 * road_length_km  # Per km traveled
    congestion_factor = 1 + (congestion / 100) * 0.5  # Idling increases emissions
    return round(vehicle_count * base_per_vehicle * congestion_factor * 1.1, 2) 

def calculate_aqi(pm25, pm10, no2):
    def sub_index(value, breakpoints):
        for bp_low, bp_high, aqi_low, aqi_high in breakpoints:
            if bp_low <= value <= bp_high:
                return round(((aqi_high - aqi_low) / (bp_high - bp_low)) * (value - bp_low) + aqi_low)
        return 500 if value > breakpoints[-1][1] else 0

    # US EPA breakpoints (standard)
    pm25_bp = [(0, 12, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150), (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500)]
    pm10_bp = [(0, 54, 0, 50), (54.1, 154, 51, 100), (154.1, 254, 101, 150), (254.1, 354, 151, 200), (354.1, 424, 201, 300), (424.1, 604, 301, 500)]
    no2_bp = [(0, 53, 0, 50), (53.1, 100, 51, 100), (100.1, 360, 101, 150), (360.1, 649, 151, 200), (649.1, 1249, 201, 300), (1249.1, 2049, 301, 500)]

    return max(
        sub_index(pm25, pm25_bp),
        sub_index(pm10, pm10_bp),
        sub_index(no2, no2_bp)
    )

# ---------------------------------------------------
# Main loop (Streaming every 5 seconds)
# ---------------------------------------------------
print("🚀 Starting Azure Event Hub Producer... sending every 5 seconds to Event Hub")

with producer:
    while True:
        current_hour = datetime.now().hour
        timestamp = datetime.now().isoformat()

        batch = producer.create_batch()  # One batch for all data

        for loc in locations:
            # Traffic simulation (realistic adjustments)
            loc["congestion"] = smooth_change(loc["congestion"])

            # Peak hours boost (Cairo peaks: 7-10 AM, 4-8 PM)
            if (7 <= current_hour <= 10) or (16 <= current_hour <= 20):
                loc["congestion"] = min(100, loc["congestion"] + random.randint(10, 20))

            congestion = loc["congestion"]
            avg_speed = round(max(20, 80 - congestion * 0.8 + random.uniform(-5, 5)), 1)  # Min 20 km/h in heavy traffic
            vehicle_count = int(50 + (congestion * 8) + random.uniform(-10, 20))  # More realistic: 200-500 in peak for 5km

            public_transport_share = round(random.uniform(15, 40), 1)  # Higher in Cairo
            accident_rate = round(random.uniform(0.1, 0.8), 2)  # WHO: Egypt avg 0.3-0.5%

            traffic_data = {
                "sensor_id": loc["sensor_id"],
                "timestamp": timestamp,
                "location_id": loc["location_id"],
                "city": loc["city"],
                "avg_speed_kmh": avg_speed,
                "vehicle_count": vehicle_count,
                "congestion_level": congestion,
                "road_length_km": loc["road_length_km"],
                "traffic_density": round(vehicle_count / loc["road_length_km"], 1),
                "peak_hour": current_hour,
                "public_transport_share": public_transport_share,
                "accident_rate": accident_rate,
                "emission_estimate": estimate_emission(congestion, vehicle_count, loc["road_length_km"]),
                "lat": loc["lat"],
                "lon": loc["lon"]
            }

            # Air quality simulation (realistic base for Egypt: dust + traffic)
            base_pm25 = 25 + random.uniform(5, 15)  # Cairo avg 40-60
            pm25 = round(base_pm25 + congestion * 0.3 + random.uniform(-3, 3), 1)
            pm10 = round(base_pm25 * 1.5 + congestion * 0.4 + random.uniform(-5, 5), 1)  # PM10 higher due to dust
            no2 = round(25 + congestion * 0.4 + random.uniform(-2, 2), 1)  # Traffic NO2
            hour_factor = abs(12 - current_hour) * 1.5  # Lower pollution midday
            o3 = round(max(10, 50 - congestion * 0.2 - hour_factor + random.uniform(-5, 5)), 1)

            aqi = calculate_aqi(pm25, pm10, no2)

            air_data = {
                "pm25": pm25,
                "pm10": pm10,
                "no2": no2,
                "co": round(0.3 + congestion * 0.01 + random.uniform(0, 0.2), 2),  # ppm
                "o3": o3,
                "air_quality_index": aqi,
            }

            # Merge traffic and air into one record (no 'type' column)
            merged_data = {**traffic_data, **air_data}

            # Add merged data to batch with partition_key = sensor_id for better distribution
            event = EventData(json.dumps(merged_data))
            batch.add(event)


            logging.info(
                f"{loc['city']} | Cong={congestion:.1f}% | Speed={avg_speed} km/h | "
                f"Veh={vehicle_count} | AQI={aqi} | PM2.5={pm25} | Emission={traffic_data['emission_estimate']} kg"
            )

        # Send batch
        producer.send_batch(batch)
        logging.info("✅ Batch sent to Azure Event Hub. Waiting 5 seconds...\n")
        time.sleep(5)