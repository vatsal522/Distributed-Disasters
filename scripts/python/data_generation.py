import pandas as pd
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Generate data for the vehicles table
def generate_vehicles_data():
    return {
        "vehicle_id": list(range(1, 51)),
        "model": [fake.word().capitalize() + " Model " + random.choice(["X", "Y", "Z"]) for _ in range(50)],
        "manufacturer": [random.choice(["Tesla", "Waymo", "Cruise", "Uber", "Ford", "Toyota", "Nissan"]) for _ in range(50)],
        "autonomy_level": [random.randint(1, 5) for _ in range(50)],
        "battery_level": [f"{random.randint(50, 100)}%" for _ in range(50)],
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

# Create DataFrames
vehicles_df = pd.DataFrame(generate_vehicles_data())
vehicle_status_df = pd.DataFrame(generate_vehicle_status_data())
routes_df = pd.DataFrame(generate_routes_data())
control_commands_df = pd.DataFrame(generate_control_commands_data())
collision_warnings_df = pd.DataFrame(generate_collision_warnings_data())
road_sensors_df = pd.DataFrame(generate_road_sensors_data())
traffic_signals_df = pd.DataFrame(generate_traffic_signals_data())

# Display all tables
# Save all the generated data into an Excel file with separate sheets for each table
with pd.ExcelWriter("database_tables.xlsx") as writer:
    vehicles_df.to_excel(writer, sheet_name="Vehicles", index=False)
    vehicle_status_df.to_excel(writer, sheet_name="Vehicle_Status", index=False)
    routes_df.to_excel(writer, sheet_name="Routes", index=False)
    control_commands_df.to_excel(writer, sheet_name="Control_Commands", index=False)
    collision_warnings_df.to_excel(writer, sheet_name="Collision_Warnings", index=False)
    road_sensors_df.to_excel(writer, sheet_name="Road_Sensors", index=False)
    traffic_signals_df.to_excel(writer, sheet_name="Traffic_Signals", index=False)

print("Excel file 'database_tables.xlsx' has been created successfully.")

