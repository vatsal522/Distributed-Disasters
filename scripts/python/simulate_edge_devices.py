import pandas as pd
import random
from faker import Faker

from confluent_kafka import Producer
import json

# Initialize Faker
fake = Faker()

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Change this to your Kafka server
}

producer = Producer(conf)

# Generate data for the vehicles table
def generate_vehicles_data():
    return {
        "vehicle_id": list(range(1, 51)),
        "model": [fake.word().capitalize() + " Model " + random.choice(["X", "Y", "Z"]) for _ in range(50)],
        "manufacturer": [random.choice(["Tesla", "Waymo", "Cruise", "Uber", "Ford", "Toyota", "Nissan"]) for _ in range(50)],
        "autonomy_level": [random.randint(1, 5) for _ in range(50)],
        "battery_level": [f"{random.randint(50, 100)}" for _ in range(50)],
        "latitude": [f"{fake.latitude():.6f}" for _ in range(50)],
        "longitude": [f"{fake.longitude():.6f}" for _ in range(50)],
        "status": [random.choice(["Active", "In Transit", "Idle", "Charging"]) for _ in range(50)]
    }

# Generate data for the vehicle_status table
def generate_vehicle_status_data():
    return {
        "vehicle_id": [random.randint(1, 50) for _ in range(50)],
        "timestamp": [fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S") for _ in range(50)],
        "speed": [random.randint(0, 120) for _ in range(50)],
        "direction": [random.choice(["North", "South", "East", "West"]) for _ in range(50)],
        "proximity_alert": [random.choice(["None", "Car ahead", "Pedestrian near", "Obstacle detected"]) for _ in range(50)],
        "road_condition": [random.choice(["Dry", "Wet", "Snowy", "Icy"]) for _ in range(50)],
        "latitude": [f"{fake.latitude():.6f}" for _ in range(50)],
        "longitude": [f"{fake.longitude():.6f}" for _ in range(50)]
    }

# Generate data for the routes table
def generate_routes_data():
    return {
        "route_id": list(range(1, 51)),
        "vehicle_id": [random.randint(1, 50) for _ in range(50)],
        "origin_latitude": [f"{fake.latitude():.6f}" for _ in range(50)],
        "origin_longitude": [f"{fake.longitude():.6f}" for _ in range(50)],
        "destination_latitude": [f"{fake.latitude():.6f}" for _ in range(50)],
        "destination_longitude": [f"{fake.longitude():.6f}" for _ in range(50)],
        "route_points": [[f"{fake.latitude():.6f},{fake.longitude():.6f}" for _ in range(5)] for _ in range(50)]
    }

# Generate data for the control_commands table
def generate_control_commands_data():
    return {
        "command_id": list(range(1, 51)),
        "vehicle_id": [random.randint(1, 50) for _ in range(50)],
        "timestamp": [fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S") for _ in range(50)],
        "command_type": [random.choice(["Change Speed", "Change Route", "Stop"]) for _ in range(50)],
        "details": [fake.sentence() for _ in range(50)]
    }

# Generate data for the collision_warnings table
def generate_collision_warnings_data():
    return {
        "warning_id": list(range(1, 51)),
        "vehicle_id": [random.randint(1, 50) for _ in range(50)],
        "timestamp": [fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S") for _ in range(50)],
        "latitude": [f"{fake.latitude():.6f}" for _ in range(50)],
        "longitude": [f"{fake.longitude():.6f}" for _ in range(50)],
        "severity": [random.choice(["Low", "Medium", "High"]) for _ in range(50)]
    }

# Generate data for the road_sensors table
def generate_road_sensors_data():
    return {
        "sensor_id": list(range(1, 51)),
        "latitude": [f"{fake.latitude():.6f}" for _ in range(50)],
        "longitude": [f"{fake.longitude():.6f}" for _ in range(50)],
        "sensor_type": [random.choice(["Proximity", "Traffic Light", "Weather"]) for _ in range(50)],
        "status": [random.choice(["Active", "Faulty"]) for _ in range(50)],
        "last_updated": [fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S") for _ in range(50)]
    }

# Generate data for the traffic_signals table
def generate_traffic_signals_data():
    return {
        "signal_id": list(range(1, 51)),
        "latitude": [f"{fake.latitude():.6f}" for _ in range(50)],
        "longitude": [f"{fake.longitude():.6f}" for _ in range(50)],
        "status": [random.choice(["Red", "Yellow", "Green"]) for _ in range(50)],
        "last_updated": [fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S") for _ in range(50)]
    }

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def push_to_kafka(topic, data):
    for record in data:
        producer.produce(topic, value=json.dumps(record), callback=delivery_report)
        producer.poll(0)

    producer.flush()

# Convert data to list of dictionaries
def convert_to_list_of_dicts(data):
    return [dict(zip(data, t)) for t in zip(*data.values())]

vehicles_data = generate_vehicles_data()
push_to_kafka('vehicles', convert_to_list_of_dicts(vehicles_data))

for i in range(50):
    vehicle_status_data = generate_vehicle_status_data()
    routes_data = generate_routes_data()
    control_commands_data = generate_control_commands_data()
    collision_warnings_data = generate_collision_warnings_data()
    road_sensors_data = generate_road_sensors_data()
    traffic_signal_data = generate_traffic_signals_data()


    push_to_kafka('vehicle_status', convert_to_list_of_dicts(vehicle_status_data))
    push_to_kafka('routes', convert_to_list_of_dicts(routes_data))
    push_to_kafka('control_commands', convert_to_list_of_dicts(control_commands_data))
    push_to_kafka('collision_warnings', convert_to_list_of_dicts(collision_warnings_data))
    push_to_kafka('road_sensors', convert_to_list_of_dicts(road_sensors_data))
    push_to_kafka('traffic_signals', convert_to_list_of_dicts(traffic_signal_data))
