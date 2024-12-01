import osmnx as ox
import pandas as pd
from datetime import datetime, timedelta
import time

from faker import Faker
import random

from confluent_kafka import Producer
import json

# Initialize Faker
fake = Faker()

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Change this to your Kafka server
}

producer = Producer(conf)

# Bounding box for San Francisco (approximate)
SF_LAT_MIN = 37.708
SF_LAT_MAX = 37.832
SF_LON_MIN = -123.000
SF_LON_MAX = -122.356

SF_ROAD_COORDINATES = pd.DataFrame()
SF_INTERSECTION_COORDINATES = pd.DataFrame()

def generate_sf_data():
    # Specify the state or city for which you want traffic signal data
    state_or_city = "San Francisco, USA"

    # Extract the road network for the specified location
    road_network = ox.graph_from_place(state_or_city, network_type="drive")

    # Get intersections (nodes where roads meet)
    intersections = ox.graph_to_gdfs(road_network, nodes=True, edges=False)

    # Extract latitude and longitude of intersections
    traffic_signal_data = intersections[['x', 'y']].rename(columns={'x': 'Latitude', 'y': 'Longitude'})

    edges = ox.graph_to_gdfs(road_network, nodes=False, edges=True)

    # Extract latitude and longitude of road segments
    road_coordinates = []
    for _, row in edges.iterrows():
        if 'geometry' in row:
            coords = list(row['geometry'].coords)
            for coord in coords:
                road_coordinates.append((coord[1], coord[0]))  # (latitude, longitude)

    global SF_ROAD_COORDINATES
    global SF_INTERSECTION_COORDINATES
    SF_ROAD_COORDINATES = pd.DataFrame(road_coordinates, columns=['Latitude', 'Longitude'])
    SF_INTERSECTION_COORDINATES = traffic_signal_data[['Latitude', 'Longitude']].reset_index(drop=True)

def pick_random_sf_coordinates():
    if len(SF_ROAD_COORDINATES) == 0 or len(SF_INTERSECTION_COORDINATES) == 0:
        generate_sf_data()

    road_or_intersection = random.choice(["road", "intersection"])
    if road_or_intersection == "road":
        return SF_ROAD_COORDINATES.sample().values[0]
    else:
        return SF_INTERSECTION_COORDINATES.sample().values[0]


def pick_random_vehicle_id():
    return random.randint(1, 100)

# Function to generate a random timestamp within a given range
def generate_random_timestamp(start, end):
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# Generate data for the vehicles table
def generate_vehicles_data():
    data = []
    for vehicle_id in range(1, 101):
        lat, lon = pick_random_sf_coordinates()
        vehicle_data = {
            "vehicle_id": vehicle_id,
            "model": fake.word().capitalize() + " Model " + random.choice(["X", "Y", "Z"]),
            "manufacturer": random.choice(["Tesla", "Waymo", "Cruise", "Uber", "Ford", "Toyota", "Nissan"]),
            "autonomy_level": random.randint(1, 5),
            "battery_level": f"{random.randint(50, 100)}",
            "latitude": f"{lat:.6f}",
            "longitude": f"{lon:.6f}",
            "status": random.choice(["Active", "In Transit", "Idle", "Charging"])
        }
        data.append(vehicle_data)
    return data

# Generate data for the vehicle_status table
def generate_vehicle_status_data():
    data = []
    for i in range(10000):
        lat, lon = pick_random_sf_coordinates()
        vehicle_status = {
            "vehicle_id": pick_random_vehicle_id(),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "speed": random.randint(0, 120),
            "direction": random.choice(["North", "South", "East", "West"]),
            "proximity_alert": random.choice(["None", "Car ahead", "Pedestrian near", "Obstacle detected"]),
            "road_condition": random.choice(["Dry", "Wet", "Snowy", "Icy"]),
            "latitude": f"{lat:.6f}",
            "longitude": f"{lon:.6f}",
        }
        data.append(vehicle_status)
    return data

# Generate data for the routes table
def generate_routes_data():
    data = []
    for i in range(1000):
        vehicle_id = pick_random_vehicle_id()
        route_id = str(i)+"-"+str(vehicle_id)+"-"+str(time.time())
        origin_lat, origin_lon = pick_random_sf_coordinates()
        destination_lat, destination_lon = pick_random_sf_coordinates()
        route_points = [f"{pick_random_sf_coordinates()[0]:.6f},{pick_random_sf_coordinates()[1]:.6f}" for _ in range(5)]
        route_data = {
            "route_id": route_id,
            "vehicle_id": vehicle_id,
            "origin_latitude": f"{origin_lat:.6f}",
            "origin_longitude": f"{origin_lon:.6f}",
            "destination_latitude": f"{destination_lat:.6f}",
            "destination_longitude": f"{destination_lon:.6f}",
            "route_points": route_points
        }
        data.append(route_data)
    return data

# Generate data for the control_commands table
def generate_control_commands_data():
    data = []
    for i in range(10000):
        vehicle_id = pick_random_vehicle_id()
        command_id = str(i)+"-"+str(vehicle_id)+"-"+str(time.time())
        command_data = {
            "command_id": command_id,
            "vehicle_id": vehicle_id,
            "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "command_type": random.choice(["Change Speed", "Change Route", "Stop"]),
            "details": fake.sentence()
        }
        data.append(command_data)
    return data

# Generate data for the collision_warnings table
def generate_collision_warnings_data():
    data = []
    for i in range(100):
        lat, lon = pick_random_sf_coordinates()
        vehicle_id = pick_random_vehicle_id()
        warning_id = str(i)+"-"+str(vehicle_id)+"-"+str(time.time())
        collision_warning = {
            "warning_id": warning_id,
            "vehicle_id": vehicle_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "latitude": f"{lat:.6f}",
            "longitude": f"{lon:.6f}",
            "severity": random.choice(["Low", "Medium", "High"])
        }
        data.append(collision_warning)
    return data

# Generate data for the road_sensors table
def generate_road_sensors_data():
    data = []
    for i in range(10000):
        lat, lon = pick_random_sf_coordinates()
        road_sensor = {
            "sensor_id":  i,
            "latitude": f"{lat:.6f}",
            "longitude": f"{lon:.6f}",
            "sensor_type": random.choice(["Proximity", "Traffic Light", "Weather"]),
            "status": random.choice(["Active", "Faulty"]),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        data.append(road_sensor)
    return data

# Generate data for the traffic_signals table
def generate_traffic_signals_data():
    data = []
    for signal_id in range(1000):
        lat, lon = SF_INTERSECTION_COORDINATES.sample().values[0]
        traffic_signal = {
            "signal_id": signal_id,
            "latitude": f"{lat:.6f}",
            "longitude": f"{lon:.6f}",
            "status": random.choice(["Red", "Yellow", "Green"]),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        data.append(traffic_signal)
    return data

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

# Generate the parent data first
vehicles_data = generate_vehicles_data()
push_to_kafka('vehicles', vehicles_data)

vehicle_status_data = generate_vehicle_status_data()
routes_data = generate_routes_data()
control_commands_data = generate_control_commands_data()
collision_warnings_data = generate_collision_warnings_data()
road_sensors_data = generate_road_sensors_data()
traffic_signal_data = generate_traffic_signals_data()


push_to_kafka('vehicle_status', vehicle_status_data)
push_to_kafka('routes', (routes_data))
push_to_kafka('control_commands', (control_commands_data))
push_to_kafka('collision_warnings', (collision_warnings_data))
push_to_kafka('road_sensors', (road_sensors_data))
push_to_kafka('traffic_signals', (traffic_signal_data))
